/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif

#include <gtest/gtest.h>
#define protected public
#define private public
#include "mittest/shared_storage/simple_server/test_storage_cache_common_util.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "unittest/share/schema/mock_schema_service.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "share/ob_vec_index_builder_util.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_sql_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#undef private
#undef protected
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/utility/ob_defer.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_cluster_version.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace sslog
{


} // namespace sslog
} // namespace oceanbase


using namespace oceanbase::transaction;
using namespace oceanbase::storage;

const char *get_per_file_test_name() 
{
  return "test_storage_cache_policy_cold_";
}
namespace oceanbase
{
namespace unittest
{

class MockBlockPolicySchemaService final : public share::schema::ObMultiVersionSchemaService
{
public:
  explicit MockBlockPolicySchemaService(const share::schema::ObTableSchema &table_schema)
    : table_schema_(table_schema)
  {}

  int get_tenant_schema_guard(
      const uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard &guard,
      int64_t tenant_schema_version,
      int64_t sys_schema_version,
      const share::schema::ObMultiVersionSchemaService::RefreshSchemaMode refresh_schema_mode) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(tenant_schema_version, sys_schema_version, refresh_schema_mode);
    share::schema::SchemaObj schema_obj;
    if (OB_FAIL(guard.reset())) {
      LOG_WARN("fail to reset mock schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(guard.init(share::schema::ObSimpleSchemaMode::ORIGINAL))) {
      LOG_WARN("fail to init mock schema guard", KR(ret), K(tenant_id));
    } else {
      guard.schema_service_ = this;
      guard.tenant_id_ = tenant_id;
      guard.schema_guard_type_ = share::schema::ObSchemaGetterGuard::TENANT_SCHEMA_GUARD;
      schema_obj.schema_type_ = share::schema::TABLE_SCHEMA;
      schema_obj.tenant_id_ = table_schema_.get_tenant_id();
      schema_obj.schema_id_ = table_schema_.get_table_id();
      schema_obj.schema_ = const_cast<share::schema::ObTableSchema *>(&table_schema_);
      if (OB_FAIL(guard.schema_objs_.push_back(schema_obj))) {
        LOG_WARN("fail to inject table schema into mock schema guard",
            KR(ret), K(tenant_id), K(schema_obj.schema_id_));
      }
    }
    return ret;
  }

private:
  const share::schema::ObTableSchema &table_schema_;
};

// ----------------------------------------------------------------------------
// 五类 LS inner tablet 在所有缓存模式下固定为 HOT，且不能被修改为其他状态
//
// 场景：不向 tablet_status_map_ 写入任何 LS inner tablet 状态，直接查询五类固定 ID。
// 操作：查询有效状态和 HOT/COLD 判定，尝试修改状态，再触发一次 force refresh。
// 校验：有效状态始终为 HOT；修改状态返回 OB_NOT_SUPPORTED；force refresh 生成五个 group task。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_ls_inner_tablets_are_always_hot)
{
  LOG_INFO("========== test_ls_inner_tablets_are_always_hot begin ==========");
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);

  // 逐一验证五类 LS inner tablet，确保判定不依赖 schema policy 或状态 map。
  for (int64_t i = 0; i < ObLS::TOTAL_INNER_TABLET_NUM; ++i) {
    const common::ObTabletID tablet_id(ObLS::INNER_TABLET_ID_LIST[i]);
    PolicyStatus policy_status = PolicyStatus::MAX_STATUS;
    bool is_hot = false;
    bool is_hot_or_macro_hot = false;
    bool is_cold = true;
    LOG_INFO("step 1: verify fixed ls inner tablet policy", K(i), K(tablet_id));
    OK(policy_service->get_tablet_policy_status(tablet_id.id(), policy_status));
    OK(policy_service->is_hot_tablet(tablet_id.id(), is_hot));
    OK(policy_service->is_hot_or_macro_hot_tablet(tablet_id.id(), is_hot_or_macro_hot));
    OK(policy_service->is_cold_tablet(tablet_id.id(), is_cold));
    LOG_INFO("step 1 done", K(i), K(tablet_id), K(policy_status),
        K(is_hot), K(is_hot_or_macro_hot), K(is_cold));
    ASSERT_EQ(PolicyStatus::HOT, policy_status);
    ASSERT_TRUE(is_hot);
    ASSERT_TRUE(is_hot_or_macro_hot);
    ASSERT_FALSE(is_cold);

    // 固定 HOT 状态不允许通过内部状态更新接口改成 AUTO。
    const int ret = policy_service->update_tablet_status(
        tablet_id.id(), PolicyStatus::AUTO, OB_INVALID_ID);
    LOG_INFO("step 2: verify status change is rejected", K(ret), K(i), K(tablet_id));
    ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  }

  // force refresh 必须绕过 schema 枚举，显式生成五类 LS inner tablet group task。
  OK(policy_service->trigger_or_refresh_tablet(REFRESH_TYPE_FORCE_REFRESH));
  SCPTabletTaskMap &tablet_task_map = policy_service->get_tablet_tasks();
  for (int64_t i = 0; i < ObLS::TOTAL_INNER_TABLET_NUM; ++i) {
    const int64_t tablet_id = ObLS::INNER_TABLET_ID_LIST[i];
    ObStorageCacheTabletTaskHandle task_handle;
    OK(tablet_task_map.get_refactored(tablet_id, task_handle));
    ASSERT_TRUE(task_handle.is_valid());
    ASSERT_NE(nullptr, task_handle());
    LOG_INFO("step 3: verify force refresh creates ls inner tablet group task",
        K(i), K(tablet_id), K(task_handle));
    ASSERT_EQ(tablet_id, task_handle()->get_tablet_id());
    ASSERT_TRUE(task_handle()->is_hot());
  }
  LOG_INFO("========== test_ls_inner_tablets_are_always_hot end ==========");
}

class MockFollowPolicySchemaService final : public share::schema::ObMultiVersionSchemaService
{
public:
  explicit MockFollowPolicySchemaService(
      const common::ObIArray<const share::schema::ObTableSchema *> &table_schemas)
    : table_schemas_(table_schemas)
  {}

  int get_tenant_schema_guard(
      const uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard &guard,
      int64_t tenant_schema_version,
      int64_t sys_schema_version,
      const share::schema::ObMultiVersionSchemaService::RefreshSchemaMode refresh_schema_mode) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(tenant_schema_version, sys_schema_version, refresh_schema_mode);
    if (OB_FAIL(guard.reset())) {
      LOG_WARN("fail to reset follow policy schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(guard.init(share::schema::ObSimpleSchemaMode::ORIGINAL))) {
      LOG_WARN("fail to init follow policy schema guard", KR(ret), K(tenant_id));
    } else {
      guard.schema_service_ = this;
      guard.tenant_id_ = tenant_id;
      guard.schema_guard_type_ = share::schema::ObSchemaGetterGuard::TENANT_SCHEMA_GUARD;
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); ++i) {
        const share::schema::ObTableSchema *table_schema = table_schemas_.at(i);
        if (OB_ISNULL(table_schema)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("injected table schema is null", KR(ret), K(i), K(tenant_id));
        } else {
          share::schema::SchemaObj schema_obj;
          schema_obj.schema_type_ = share::schema::TABLE_SCHEMA;
          schema_obj.tenant_id_ = table_schema->get_tenant_id();
          schema_obj.schema_id_ = table_schema->get_table_id();
          schema_obj.schema_ = const_cast<share::schema::ObTableSchema *>(table_schema);
          if (OB_FAIL(guard.schema_objs_.push_back(schema_obj))) {
            LOG_WARN("fail to inject follow policy table schema",
                KR(ret), K(i), K(tenant_id), K(schema_obj.schema_id_));
          }
        }
      }
    }
    return ret;
  }

private:
  const common::ObIArray<const share::schema::ObTableSchema *> &table_schemas_;
};

class MockTableIdPolicySchemaService final : public share::schema::MockSchemaService
{
public:
  MockTableIdPolicySchemaService()
    : guard_call_count_(0),
      first_failed_guard_call_(0),
      failed_guard_call_count_(0),
      guard_error_(OB_SUCCESS)
  {}

  void inject_guard_failure(
      const int64_t first_failed_guard_call,
      const int64_t failed_guard_call_count,
      const int guard_error)
  {
    first_failed_guard_call_ = first_failed_guard_call;
    failed_guard_call_count_ = failed_guard_call_count;
    guard_error_ = guard_error;
  }

  int64_t get_guard_call_count() const { return guard_call_count_; }

  int get_tenant_schema_guard(
      const uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard &guard,
      int64_t tenant_schema_version,
      int64_t sys_schema_version,
      const share::schema::ObMultiVersionSchemaService::RefreshSchemaMode refresh_schema_mode) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(tenant_schema_version, sys_schema_version, refresh_schema_mode);
    ++guard_call_count_;
    const bool inject_failure = first_failed_guard_call_ > 0
        && guard_call_count_ >= first_failed_guard_call_
        && guard_call_count_ < first_failed_guard_call_ + failed_guard_call_count_;
    if (!is_valid_tenant_id(tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (inject_failure) {
      ret = guard_error_;
    } else if (OB_FAIL(get_schema_guard(guard))) {
      LOG_WARN("fail to get mock schema guard", KR(ret), K(tenant_id), K(guard_call_count_));
    }
    return ret;
  }

private:
  int64_t guard_call_count_;
  int64_t first_failed_guard_call_;
  int64_t failed_guard_call_count_;
  int guard_error_;
};

static int add_mock_user_tenant(
    MockTableIdPolicySchemaService &schema_service,
    const uint64_t tenant_id,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  share::schema::ObTenantSchema tenant_schema;
  share::schema::ObSysVariableSchema sys_variable_schema;
  tenant_schema.set_tenant_id(tenant_id);
  tenant_schema.set_schema_version(schema_version);
  sys_variable_schema.set_tenant_id(tenant_id);
  sys_variable_schema.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
  sys_variable_schema.set_schema_version(schema_version);
  if (OB_FAIL(tenant_schema.set_locality("auto_locality_strategy"))) {
    LOG_WARN("fail to set mock tenant locality", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_schema.add_zone("zone"))) {
    LOG_WARN("fail to add mock tenant zone", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_schema.set_tenant_name("scp_follow_scan_tenant"))) {
    LOG_WARN("fail to set mock tenant name", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service.add_tenant_schema(tenant_schema, schema_version))) {
    LOG_WARN("fail to add mock tenant schema", KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(schema_service.add_sys_variable_schema(
      sys_variable_schema, schema_version))) {
    LOG_WARN("fail to add mock sys variable schema", KR(ret), K(tenant_id), K(schema_version));
  }
  return ret;
}

static int set_mock_table_name(
    share::schema::ObTableSchema &table_schema,
    const uint64_t data_table_id,
    const char *logical_table_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(logical_table_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mock table name is null", KR(ret), K(data_table_id));
  } else if (table_schema.is_index_table()) {
    ObArenaAllocator allocator(ObMemAttr(table_schema.get_tenant_id(), "SCPMockIdxName"));
    ObString index_table_name;
    if (OB_INVALID_ID == data_table_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("mock index data table id is invalid", KR(ret), K(data_table_id));
    } else if (OB_FAIL(share::schema::ObTableSchema::build_index_table_name(
        allocator,
        data_table_id,
        ObString::make_string(logical_table_name),
        index_table_name))) {
      LOG_WARN("fail to build mock index table name",
          KR(ret), K(data_table_id), K(logical_table_name));
    } else if (OB_FAIL(table_schema.set_table_name(index_table_name))) {
      LOG_WARN("fail to set mock index table name",
          KR(ret), K(data_table_id), K(index_table_name));
    }
  } else if (OB_FAIL(table_schema.set_table_name(logical_table_name))) {
    LOG_WARN("fail to set mock table name", KR(ret), K(data_table_id), K(logical_table_name));
  }
  return ret;
}

static int init_mock_partitioned_table_schema(
    share::schema::ObTableSchema &table_schema,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t data_table_id,
    const char *table_name,
    const share::schema::ObTableType table_type,
    const share::schema::ObIndexType index_type,
    const share::schema::ObTableStateFlag table_state,
    const int64_t part_id,
    const int64_t tablet_id,
    const int64_t schema_version,
    const ObStorageCachePolicyType table_policy_type)
{
  int ret = OB_SUCCESS;
  table_schema.reset();
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_table_id(table_id);
  if (OB_INVALID_ID != data_table_id) {
    table_schema.set_data_table_id(data_table_id);
  }
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_type(table_type);
  table_schema.set_index_type(index_type);
  table_schema.set_index_status(share::schema::INDEX_STATUS_AVAILABLE);
  table_schema.set_schema_version(schema_version);
  table_schema.set_table_state_flag(table_state);
  table_schema.set_storage_cache_policy_type(table_policy_type);
  table_schema.set_part_level(share::schema::PARTITION_LEVEL_ONE);
  table_schema.set_part_num(1);
  table_schema.get_part_option().set_part_func_type(share::schema::PARTITION_FUNC_TYPE_HASH);
  if (OB_FAIL(table_schema.get_part_option().set_part_expr(
      ObString::make_string("c1")))) {
    LOG_WARN("fail to set mock partition expression", KR(ret), K(table_id));
  } else if (OB_FAIL(set_mock_table_name(table_schema, data_table_id, table_name))) {
    LOG_WARN("fail to set mock table name", KR(ret), K(table_id), K(table_name));
  } else {
    share::schema::ObPartition partition;
    partition.set_tenant_id(tenant_id);
    partition.set_table_id(table_id);
    partition.set_part_id(part_id);
    partition.set_part_idx(0);
    partition.set_tablet_id(common::ObTabletID(tablet_id));
    partition.set_schema_version(schema_version);
    partition.set_part_storage_cache_policy_type(ObStorageCachePolicyType::NONE_POLICY);
    if (OB_FAIL(table_schema.add_partition(partition))) {
      LOG_WARN("fail to add mock partition",
          KR(ret), K(table_id), K(part_id), K(tablet_id), K(schema_version));
    }
  }
  return ret;
}

// hot to cold，test evict local cache
TEST_F(ObStorageCachePolicyColdTest, test_convert_hot_to_cold)
{
  FLOG_INFO("[TEST] start test_convert_hot_auto_to_cold_evict");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  // Test case 1: hot to cold
  OK(exe_sql("create table test_hot_to_cold (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_hot_to_cold");

  OK(exe_sql("insert into test_hot_to_cold values (1)"));
  OK(exe_sql("insert into test_hot_to_cold values (2)"));
  OK(exe_sql("insert into test_hot_to_cold values (3)"));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));
  
  // 1. set hot policy, wait prewarm task finished
  OK(exe_sql("alter table test_hot_to_cold storage_cache_policy (global = 'hot');"));
  wait_task_finished(run_ctx_.tablet_id_.id());
  
  // 2. check macro block in cache
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 128> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
  FLOG_INFO("[TEST] data_block_ids", K(data_block_ids));
  FLOG_INFO("[TEST] meta_block_ids", K(meta_block_ids));
  bool is_exist = false;
  int64_t macro_count_before_evict = 0;

  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist));
      if (is_exist) {
        macro_count_before_evict++;
        FLOG_INFO("[TEST] macro block in cache before evict", K(i), K(data_block_ids.at(i)));
      }
    }
  }
  FLOG_INFO("[TEST] macro_count_before_evict", K(macro_count_before_evict));
  ASSERT_GT(macro_count_before_evict, 0);
  FLOG_INFO("[TEST] before evict, macro count in cache", K(macro_count_before_evict));
  
  // 3. convert to cold policy, should trigger evict local cache
  OK(exe_sql("alter table test_hot_to_cold storage_cache_policy (global = 'cold');"));
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  // 4. 等待定时批量任务淘汰宏块
  int64_t macro_count_after_evict = macro_count_before_evict;
  int64_t wait_time = 0;
  FLOG_INFO("[TEST] after evict, data_block_ids", K(data_block_ids));
  while (macro_count_after_evict > 0 && wait_time < 60) {
    sleep(1);
    wait_time++;
    macro_count_after_evict = 0;
    for (int i = 0; i < data_block_ids.count(); i++) {
      if (data_block_ids.at(i).is_shared_data_or_meta()) {
        OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist));
        if (is_exist) {
          macro_count_after_evict++;
          FLOG_INFO("[TEST] macro block in cache after evict", K(i), K(data_block_ids.at(i)));
        }
      }
    }
  }
  FLOG_INFO("[TEST] after evict, macro count in cache", K(macro_count_after_evict));
  ASSERT_EQ(0, macro_count_after_evict);
  OK(exe_sql("drop table if exists test_hot_to_cold"));
  FLOG_INFO("[TEST] finish test_convert_hot_to_cold");
}

TEST_F(ObStorageCachePolicyColdTest, test_convert_auto_to_cold)
{
  FLOG_INFO("[TEST] start test_convert_hot_auto_to_cold_evict");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  OK(exe_sql("drop table if exists test_auto_to_cold"));
  OK(exe_sql("alter system set default_storage_cache_policy = 'auto'"));
  OK(exe_sql("create table test_auto_to_cold (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_auto_to_cold");

  OK(exe_sql("insert into test_auto_to_cold values (1)"));
  OK(exe_sql("insert into test_auto_to_cold values (2)"));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));
  
  // 1. set auto policy
  OK(exe_sql("alter table test_auto_to_cold storage_cache_policy (global = 'auto');"));
 
  // 2. get macro block id
  ObSEArray<MacroBlockId, 128> auto_data_block_ids;
  ObSEArray<MacroBlockId, 128> auto_meta_block_ids;
  auto_data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "AutoDataBlkIDs"));
  auto_meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "AutoMetaBlkIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, auto_data_block_ids, auto_meta_block_ids));
  FLOG_INFO("[TEST] auto policy before evict, auto_data_block_ids", K(auto_data_block_ids));
  FLOG_INFO("[TEST] auto policy before evict, auto_meta_block_ids", K(auto_meta_block_ids));
  int64_t macro_count_before_evict = 0;
  bool is_exist = false;
  for (int i = 0; i < auto_data_block_ids.count(); i++) {
    if (auto_data_block_ids.at(i).is_shared_data_or_meta()) {    
      FLOG_INFO("[TEST] check macro block in cache before evict", K(i), K(auto_data_block_ids.at(i)));
      OK(macro_cache_mgr->exist(auto_data_block_ids.at(i), is_exist));
      if (is_exist) {
        macro_count_before_evict++;
        FLOG_INFO("[TEST] macro block in cache before evict", K(i), K(auto_data_block_ids.at(i)));
      }
    }
  }
  FLOG_INFO("[TEST] auto policy before evict, macro count", K(macro_count_before_evict));
  
  // 3. convert to cold policy
  OK(exe_sql("alter table test_auto_to_cold storage_cache_policy (global = 'cold');"));
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  
  // 4. check macro block evicted
  int64_t macro_count_after_evict = auto_data_block_ids.count();
  int64_t wait_time = 0;
  while(macro_count_after_evict > 0 && wait_time < 60) {
    sleep(1);
    wait_time++;
    if (wait_time % 60 == 0) {
      FLOG_INFO("[TEST] macros remaining in cache after eviction", K(macro_count_after_evict));
    }
    macro_count_after_evict = 0;
    for (int i = 0; i < auto_data_block_ids.count(); i++) {
      if (auto_data_block_ids.at(i).is_shared_data_or_meta()) {
        OK(macro_cache_mgr->exist(auto_data_block_ids.at(i), is_exist));
        if (is_exist) {
          ObSSMacroCacheMetaHandle meta_handle;
          OK(macro_cache_mgr->get_macro_cache_meta(auto_data_block_ids.at(i), is_exist, meta_handle));
          macro_count_after_evict++;
          FLOG_INFO("[TEST] macro block in cache after evict", K(i), K(auto_data_block_ids.at(i)), K(meta_handle()->get_is_write_cache()));
        }
      }
    }
  }
  ASSERT_EQ(0, macro_count_after_evict);
  OK(exe_sql("drop table if exists test_auto_to_cold"));
  FLOG_INFO("[TEST] finish test_convert_auto_to_cold");
}

// cold to hot，macro will become hot macro
TEST_F(ObStorageCachePolicyColdTest, test_convert_cold_to_hot)
{
  FLOG_INFO("[TEST] start test_convert_cold_to_hot");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  OK(exe_sql("alter system set default_storage_cache_policy = 'cold'"));
  OK(exe_sql("create table test_cold_to_hot (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_cold_to_hot");

  OK(exe_sql("insert into test_cold_to_hot values (1)"));
  OK(exe_sql("insert into test_cold_to_hot values (2)"));
  OK(exe_sql("insert into test_cold_to_hot values (3)"));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));
  
  // 1. get macro block id
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 128> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
  
  // 2. convert to hot policy, wait prewarm task finished
  OK(exe_sql("alter table test_cold_to_hot storage_cache_policy (global = 'hot');"));
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  wait_task_finished(run_ctx_.tablet_id_.id());
  
  // 3. check macro block type is HOT_TABLET_MACRO_BLOCK
  check_macro_blocks_type(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK);
  
  // 4. check macro block in cache
  bool is_exist = false;
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      ObSSMacroCacheMetaHandle meta_handle;
      OK(macro_cache_mgr->get_macro_cache_meta(data_block_ids.at(i), is_exist, meta_handle));
      if (is_exist) {
        ASSERT_EQ(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, meta_handle()->cache_type_);
        FLOG_INFO("[TEST] macro block type is HOT_TABLET_MACRO_BLOCK", K(data_block_ids.at(i)), 
            K(meta_handle()->cache_type_));
      }
    }
  }
  FLOG_INFO("[TEST] finish test_convert_cold_to_hot");
}

// cold to auto，if prewarm is triggered, will be stored in cache
TEST_F(ObStorageCachePolicyColdTest, test_convert_cold_to_auto)
{
  FLOG_INFO("[TEST] start test_convert_cold_to_auto");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  OK(exe_sql("alter system set default_storage_cache_policy = 'cold'"));
  OK(exe_sql("create table test_cold_to_auto (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_cold_to_auto");

  OK(exe_sql("insert into test_cold_to_auto values (1)"));
  OK(exe_sql("insert into test_cold_to_auto values (2)"));
  OK(exe_sql("insert into test_cold_to_auto values (3)"));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));
  
  // 1. trigger refresh to ensure tablet status is updated to cold in tablet_status_map_
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  
  // 2. get macro block id
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 128> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
  
  // 3. check macro block not in cache
  bool is_exist = false;
  int64_t macro_count_before = 0;
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist));
      if (is_exist) {
        macro_count_before++;
        FLOG_INFO("[TEST] macro block in cache before convert cold to auto", K(i), K(data_block_ids.at(i)));
      }
    }
  }
  FLOG_INFO("[TEST] cold policy, macro count is not in cache", K(macro_count_before));
  
  // 4. convert to auto policy
  OK(exe_sql("alter table test_cold_to_auto storage_cache_policy (global = 'auto');"));
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  
  // 5. trigger query to trigger prewarm
  OK(exe_sql("select * from test_cold_to_auto"));
  
  // 6. check macro block in cache
  int64_t macro_count_after = 0;
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist));
      if (is_exist) {
        macro_count_after++;
        // check macro block type is MACRO_BLOCK
        ObSSMacroCacheMetaHandle meta_handle;
        bool meta_exist = false;
        OK(macro_cache_mgr->get_macro_cache_meta(data_block_ids.at(i), meta_exist, meta_handle));
        if (meta_exist) {
          ASSERT_EQ(ObSSMacroCacheType::MACRO_BLOCK, meta_handle()->cache_type_);
        }
      }
    }
  }
  FLOG_INFO("[TEST] auto policy after prewarm, macro count in cache", K(macro_count_after));
  FLOG_INFO("[TEST] finish test_convert_cold_to_auto");
}

// cold policy is not persisted in macro cache or micro cache, but remains in KVCACHE
// 场景：COLD 策略下验证缓存行为，等待预读结束时同时观察异步 put，区分读取未完成和写缓存未完成。
TEST_F(ObStorageCachePolicyColdTest, test_cold_policy_cache_behavior)
{
  SCOPED_TRACE("COLD 策略下验证缓存行为，等待预读结束时同时观察异步 put，区分读取未完成和写缓存未完成。");
  FLOG_INFO("[TEST] start test_cold_policy_cache_behavior");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  OK(exe_sql("alter system set default_storage_cache_policy = 'cold'"));
  OK(exe_sql("create table test_cold_cache_behavior (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_cold_cache_behavior");

  OK(exe_sql("insert into test_cold_cache_behavior values (1)"));
  OK(exe_sql("insert into test_cold_cache_behavior values (2)"));
  OK(exe_sql("insert into test_cold_cache_behavior values (3)"));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));

  // 1. get macro block id
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 128> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));

  // 2. trigger refresh to ensure tablet status is updated to cold in tablet_status_map_
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));

  // 3. wait for the asynchronous cold tablet task to evict macro cache
  const int64_t evict_timeout_us = 60 * 1000 * 1000L; // 60s
  const int64_t evict_start_us = ObTimeUtility::current_time();
  bool all_macro_cache_evicted = false;
  while (!all_macro_cache_evicted
      && ObTimeUtility::current_time() - evict_start_us < evict_timeout_us) {
    all_macro_cache_evicted = true;
    for (int64_t i = 0; i < data_block_ids.count(); ++i) {
      if (data_block_ids.at(i).is_shared_data_or_meta()) {
        bool is_exist = false;
        OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist));
        if (is_exist) {
          all_macro_cache_evicted = false;
          break;
        }
      }
    }
    if (!all_macro_cache_evicted) {
      ob_usleep(100 * 1000); // 100ms
    }
  }
  ASSERT_TRUE(all_macro_cache_evicted);

  // 4. try to aio_read twice, no micro cache hit
  FLOG_INFO("[TEST] try to aio_read twice, no micro cache hit");
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  ObStorageCacheHitStat entry;
  local_cache_service->get_local_cache_tablet_stat(run_ctx_.tablet_id_, entry);
  int old_miss_cnt = entry.get_miss_cnt();
  FLOG_INFO("[TEST] old_miss_cnt", K(old_miss_cnt));
  char read_buf[1024] = {0};
  int miss_cnt = 0;
  
  // First round: read all blocks
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      ObSSShareMacroReader share_macro_reader;
      ObStorageObjectReadInfo read_info;
      read_info.macro_block_id_ = data_block_ids.at(i);
      read_info.offset_ = 0;
      read_info.size_ = 512;
      read_info.buf_ = read_buf;
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
      read_info.mtl_tenant_id_ = MTL_ID();
      read_info.set_effective_tablet_id(run_ctx_.tablet_id_);
      ObStorageObjectHandle object_handle;
      IGNORE_RETURN(tenant_kvcache_mgr->erase_macro_block(read_info.macro_block_id_));
      OK(share_macro_reader.aio_read(read_info, object_handle));
      OK(object_handle.wait());
      FLOG_INFO("test aio_read", K(read_info), K(object_handle.get_data_size()));
      miss_cnt++;
    }
  }
  local_cache_service->get_local_cache_tablet_stat(run_ctx_.tablet_id_, entry);
  FLOG_INFO("[TEST] first round miss_cnt", K(old_miss_cnt + miss_cnt), K(entry.get_miss_cnt()));
  
  // Second round: read all blocks again to test micro cache miss
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      ObSSShareMacroReader share_macro_reader;
      ObStorageObjectReadInfo read_info;
      read_info.macro_block_id_ = data_block_ids.at(i);
      read_info.offset_ = 0;
      read_info.size_ = 512;
      read_info.buf_ = read_buf;
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
      read_info.mtl_tenant_id_ = MTL_ID();
      read_info.set_effective_tablet_id(run_ctx_.tablet_id_);
      ObStorageObjectHandle object_handle;
      IGNORE_RETURN(tenant_kvcache_mgr->erase_macro_block(read_info.macro_block_id_));
      OK(share_macro_reader.aio_read(read_info, object_handle));
      OK(object_handle.wait());
      FLOG_INFO("test aio_read", K(read_info), K(object_handle.get_data_size()));
      miss_cnt++;
    }
  }
  FLOG_INFO("[TEST] total miss_cnt", K(miss_cnt));
  local_cache_service->get_local_cache_tablet_stat(run_ctx_.tablet_id_, entry);
  ASSERT_EQ(old_miss_cnt + miss_cnt, entry.get_miss_cnt());

  // 5. push cold policy file to preread queue
  FLOG_INFO("[TEST] test cold policy in preread task");
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(file_manager->push_to_preread_queue(data_block_ids.at(i), run_ctx_.tablet_id_));
    }
  }
  // 6. wait preread_task read cold policy file to macro memory cache.
  int ret = OB_SUCCESS;
  int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L; // 20s
  while ((preread_cache_mgr.preread_queue_.size() != 0) ||
        (preread_task.segment_files_.count() != 0) ||
        (preread_task.free_list_.get_curr_total() != file_manager->preread_cache_mgr_.preread_task_.max_pre_read_parallelism_)) {
    ob_usleep(1000);
    if (timeout_us + start_us < ObTimeUtility::current_time()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("waiting time is too long", KR(ret),
          KR(preread_cache_mgr.preread_queue_.size()), KR(preread_task.segment_files_.count()),
          KR(preread_task.async_read_list_.get_curr_total()), KR(preread_task.pending_put_count_));
      break;
    }
  }
  
  // 7. check macro block not in macro cache
  bool is_macro_cache_exist = false;
  int64_t macro_cache_count = 0;
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(macro_cache_mgr->exist(data_block_ids.at(i), is_macro_cache_exist));
      if (is_macro_cache_exist && data_block_ids.at(i).storage_object_type() != ObStorageObjectType::SHARED_MINI_DATA_MACRO) {
        macro_cache_count++;
      }
    }
  }
  ASSERT_EQ(0, macro_cache_count);
  FLOG_INFO("[TEST] cold policy, macro cache count", K(macro_cache_count));
  
  // L2 不缓存普通 COLD 宏块，但 L1 仍允许预读写入；分别检查两级缓存，避免混淆策略。
  // 8. check macro block in memory cache
  bool is_kvcache_exist = false;
  int64_t kvcache_count = 0;
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(tenant_kvcache_mgr->check_exist(data_block_ids.at(i), is_kvcache_exist));
      if (is_kvcache_exist) {
        kvcache_count++;
      }
    }
  }
  ASSERT_GT(kvcache_count, 0);
  OK(exe_sql("alter system set default_storage_cache_policy = 'auto'"));
  FLOG_INFO("[TEST] cold policy, KVCACHE count", K(kvcache_count));
  FLOG_INFO("[TEST] finish test_cold_policy_cache_behavior");
}

// cold policy evicts write cache with requeue until all write cache is cleared
TEST_F(ObStorageCachePolicyColdTest, test_cold_evict_write_cache_task)
{
  FLOG_INFO("[TEST] start test_cold_evict_write_cache_task");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  
  // 1. create table and insert data
  OK(exe_sql("create table test_cold_write_cache (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_cold_write_cache");
  const uint64_t tablet_id = run_ctx_.tablet_id_.id();
  const uint64_t server_id = 1;
  OK(exe_sql("insert into test_cold_write_cache values (1)"));
  OK(exe_sql("insert into test_cold_write_cache values (2)"));
  OK(exe_sql("insert into test_cold_write_cache values (3)"));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));
  
  OK(exe_sql("alter table test_cold_write_cache storage_cache_policy (global = 'hot');"));
  wait_task_finished(run_ctx_.tablet_id_.id());
  
  // 2. Directly construct write_cache macro blocks to guarantee that we have write_cache, not read_cache
  const int64_t WRITE_IO_SIZE = 2 * 1024L * 1024L; // 2MB
  char write_buf[WRITE_IO_SIZE];
  memset(write_buf, 'a', WRITE_IO_SIZE);
  
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = WRITE_IO_SIZE;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  write_info.set_effective_tablet_id(tablet_id);
  
  const int64_t MACRO_BLOCK_COUNT = 3;
  ObSEArray<MacroBlockId, 16> constructed_macro_ids;
  for (int64_t i = 0; i < MACRO_BLOCK_COUNT; i++) {
    MacroBlockId macro_id;
    macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(0);
    macro_id.set_tenant_seq(100 + i);
    ASSERT_TRUE(macro_id.is_valid());
    
    // Write to macro cache as write_cache
    ObStorageObjectHandle write_object_handle;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
    ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info, write_object_handle));
    ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
    write_object_handle.reset();
    
    ASSERT_EQ(OB_SUCCESS, constructed_macro_ids.push_back(macro_id));
    FLOG_INFO("[TEST] constructed write_cache macro block", K(i), K(macro_id));
  }

  // 3. Verify these macro blocks are write_cache
  bool is_exist = false;
  int64_t write_cache_macro_count = 0;
  for (int i = 0; i < constructed_macro_ids.count(); i++) {
    const MacroBlockId &macro_id = constructed_macro_ids.at(i);
    OK(macro_cache_mgr->exist(macro_id, is_exist));
    ASSERT_TRUE(is_exist);
    ObSSMacroCacheMetaHandle meta_handle;
    ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
    ASSERT_TRUE(meta_handle.is_valid());
    ASSERT_TRUE(meta_handle()->get_is_write_cache());
    ASSERT_EQ(tablet_id, meta_handle()->get_effective_tablet_id());
    write_cache_macro_count++;
  }
  FLOG_INFO("[TEST] verified write_cache macro blocks", K(write_cache_macro_count), K(constructed_macro_ids));
  ASSERT_EQ(MACRO_BLOCK_COUNT, write_cache_macro_count);
  
  // 4. Get initial write cache size
  int64_t initial_write_cache_size = macro_cache_mgr->get_write_cache_size();
  FLOG_INFO("[TEST] initial write cache size", K(initial_write_cache_size));
  // exist other macro blocks in write cache
  ASSERT_GT(initial_write_cache_size, WRITE_IO_SIZE * MACRO_BLOCK_COUNT);
  
  // 5. Add tablet to active_tablet_ids (required for eviction task)
  ASSERT_EQ(OB_SUCCESS, policy_service->tablet_scheduler_.active_tablet_ids_.set_refactored(tablet_id));
  
  // 6. Convert to cold policy to trigger eviction
  OK(exe_sql("alter table test_cold_write_cache storage_cache_policy (global = 'cold');"));
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  ASSERT_EQ(OB_HASH_EXIST, policy_service->get_need_evict_cold_tablet_ids().exist_refactored(tablet_id));
  policy_service->refresh_policy_scheduler_.evict_cold_tablet_task_.runTimerTask();

  // 7. Verify all constructed macros are evicted from cache
  int64_t remaining_macro_count = constructed_macro_ids.count();
  is_exist = false;
  int64_t wait_time = 0;
  while(remaining_macro_count > 0 && wait_time < 60) {
    sleep(1);
    wait_time++;
    if (wait_time % 60 == 0) {
      FLOG_INFO("[TEST] macros remaining in cache after eviction", K(remaining_macro_count));
    }
    remaining_macro_count = 0;
    for (int i = 0; i < constructed_macro_ids.count(); i++) {
      OK(macro_cache_mgr->exist(constructed_macro_ids.at(i), is_exist));
      if (is_exist) {
        remaining_macro_count++;
      }
    }
  }
  ASSERT_EQ(0, remaining_macro_count);
  OK(exe_sql("drop table if exists test_cold_write_cache"));
  FLOG_INFO("[TEST] finish test_cold_evict_write_cache_requeue");
}

// 宏块粒度（BLOCK）的存储缓存策略已禁用，保留历史测试但不执行。
// TEST_F(ObStorageCachePolicyColdTest, test_macro_block_cold_elimination)
// {
//   FLOG_INFO("[TEST] start test_macro_block_cold_elimination");
//   share::ObTenantSwitchGuard tguard;
//   bool is_exist = false;
//   OK(tguard.switch_to(run_ctx_.tenant_id_));
//   exe_prepare_sql();
//
//   ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
//   ASSERT_NE(nullptr, policy_service);
//   ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
//   ASSERT_NE(nullptr, macro_cache_mgr);
//   ObSSMemMacroCache *mem_macro_cache = MTL(ObSSMemMacroCache *);
//   ASSERT_NE(nullptr, mem_macro_cache);
//
//   OK(exe_sql("create table test_cold_macro (a int primary key, b int)"));
//   set_ls_and_tablet_id_for_run_ctx("test_cold_macro");
//
//   OK(exe_sql("insert into test_cold_macro values (1, 1)"));
//   OK(exe_sql("insert into test_cold_macro values (2, 2)"));
//   OK(exe_sql("insert into test_cold_macro values (3, 3)"));
//   OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));
//
//   FLOG_INFO("[TEST] Step 1: Created table and data, waiting for data to be persisted");
//
//   OK(exe_sql("alter table test_cold_macro storage_cache_policy (global = 'hot');"));
//   wait_task_finished(run_ctx_.tablet_id_.id());
//
//   ObSEArray<MacroBlockId, 128> data_block_ids;
//   ObSEArray<MacroBlockId, 128> meta_block_ids;
//   data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
//   meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));
//   OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
//
//   FLOG_INFO("[TEST] Step 2: Get macro blocks", K(data_block_ids.count()), K(meta_block_ids.count()));
//
//   check_macro_blocks_type(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK);
//
//   int64_t initial_macro_count = 0;
//   ObSEArray<MacroBlockId, 16> cached_macro_ids;
//   for (int i = 0; i < data_block_ids.count(); i++) {
//     if (data_block_ids.at(i).is_shared_data_or_meta()) {
//       OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist));
//       if (is_exist) {
//         initial_macro_count++;
//         OK(cached_macro_ids.push_back(data_block_ids.at(i)));
//       }
//     }
//   }
//   FLOG_INFO("[TEST] Step 3: Initial cached macro count", K(initial_macro_count), K(cached_macro_ids));
//   ASSERT_GT(initial_macro_count, 0);
//
//   OK(exe_sql("alter table test_cold_macro storage_cache_policy "
//              "(granularity = 'block', hot_retention = 10 SECOND, mixed_retention = 1 SECOND);"));
//   ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
//   FLOG_INFO("[TEST] Step 4: Switched to macro_hot time policy");
//
//   int64_t macro_count_after_policy_change = 0;
//   for (int i = 0; i < cached_macro_ids.count(); i++) {
//     OK(macro_cache_mgr->exist(cached_macro_ids.at(i), is_exist));
//     if (is_exist) {
//       macro_count_after_policy_change++;
//     }
//   }
//   FLOG_INFO("[TEST] Step 5: Macro count after policy change", K(macro_count_after_policy_change));
//   ASSERT_EQ(initial_macro_count, macro_count_after_policy_change);
//
//   const uint64_t tablet_id = run_ctx_.tablet_id_.id();
//   ASSERT_EQ(OB_SUCCESS, policy_service->tablet_scheduler_.active_tablet_ids_.set_refactored(tablet_id));
//
//   const int64_t wait_seconds = 15;
//   FLOG_INFO("[TEST] Step 6: Waiting for macros to age beyond retention time",
//             "wait_seconds", wait_seconds, "hot_retention", 1, "mixed_retention", 1);
//
//   sleep(wait_seconds);
//
//   FLOG_INFO("[TEST] Step 7 completed: Macros have aged, now triggering eliminate task");
//
//   ObStorageCacheEliminateMacroTask &eliminate_task =
//       policy_service->refresh_policy_scheduler_.eliminate_macro_task_;
//   ASSERT_EQ(OB_SUCCESS, eliminate_task.eliminate_macro_blocks());
//
//   hash::ObHashSet<MacroBlockId> &cold_macro_ids = policy_service->get_need_evict_cold_macro_block_ids();
//   int64_t cold_macro_count = cold_macro_ids.size();
//   FLOG_INFO("[TEST] Step 8: Cold macros identified by eliminate task", K(cold_macro_count), K(cold_macro_ids));
//
//   ASSERT_TRUE(cold_macro_ids.created());
//   ASSERT_GT(cold_macro_count, 0);
//
//   FLOG_INFO("[TEST] Step 9: Successfully identified cold macros, proceeding with eviction");
//
//   ObStorageCacheEvictColdMacroTask &evict_cold_macro_task =
//         policy_service->refresh_policy_scheduler_.evict_cold_macro_task_;
//   evict_cold_macro_task.runTimerTask();
//
//   int64_t evict_wait_time = 0;
//   int64_t remaining_cold_count = cold_macro_ids.size();
//   FLOG_INFO("[TEST] Start waiting for cold macro eviction", K(remaining_cold_count));
//   while(remaining_cold_count > 0 && evict_wait_time < 60) {
//     sleep(1);
//     evict_wait_time++;
//     int64_t prev_count = remaining_cold_count;
//     remaining_cold_count = cold_macro_ids.size();
//     if (remaining_cold_count != prev_count || evict_wait_time % 5 == 0) {
//       FLOG_INFO("[TEST] Waiting for cold macro eviction",
//                 K(remaining_cold_count), K(evict_wait_time), "prev_count", prev_count, "delta", prev_count - remaining_cold_count);
//     }
//   }
//
//   FLOG_INFO("[TEST] Step 10: Cold macro eviction completed", "final_cold_count", cold_macro_ids.size(), "wait_time", evict_wait_time);
//
//   int64_t evicted_count = 0;
//   int64_t still_cached_count = 0;
//   for (int i = 0; i < cached_macro_ids.count(); i++) {
//     FLOG_INFO("[TEST] check cold macro not in macro cache", K(cached_macro_ids.at(i)));
//     OK(macro_cache_mgr->exist(cached_macro_ids.at(i), is_exist));
//     FLOG_INFO("[TEST] macro exist check result", K(cached_macro_ids.at(i)), K(is_exist));
//     if (is_exist) {
//       still_cached_count++;
//       FLOG_INFO("[TEST] Warning: Cold macro still exists in cache after eviction!", "macro_id", cached_macro_ids.at(i));
//     } else {
//       evicted_count++;
//     }
//     ASSERT_FALSE(is_exist);
//   }
//   FLOG_INFO("[TEST] Final eviction verification", K(evicted_count), K(still_cached_count),
//             "total_count", cached_macro_ids.count());
//
//   OK(exe_sql("drop table if exists test_cold_macro"));
//   FLOG_INFO("[TEST] finish test_macro_block_cold_conversion_and_real_elimination");
// }

TEST_F(ObStorageCachePolicyColdTest, test_cold_merge)
{
  FLOG_INFO("[TEST] start test_cold_merge");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  OK(exe_sql("create table test_cold_merge (a varchar(4096), b varchar(4096)) storage_cache_policy (global = 'cold');"));
  set_ls_and_tablet_id_for_run_ctx("test_cold_merge");
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  OK(exe_sql("insert into test_cold_merge select randstr(4096, random()), randstr(4096, random()) from table(generator(1024))"));
  FLOG_INFO("[TEST] Step 1: create table and insert data", K(run_ctx_));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 128> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
  FLOG_INFO("[TEST] Step 2: Get macro blocks", K(data_block_ids.count()), K(meta_block_ids.count()));
  check_macro_blocks_type(ObSSMacroCacheType::MACRO_BLOCK);
  FLOG_INFO("[TEST] Step 3: check cold mini/minor/major not in local cache");
  bool is_exist = false;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      FLOG_INFO("[TEST] check cold mini/minor/major not in local cache", K(data_block_ids.at(i)));
      OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist));
      ASSERT_FALSE(is_exist);
      tenant_file_mgr->is_exist_remote_file(data_block_ids.at(i), 0/*ls_epoch_id*/, is_exist);
      ASSERT_TRUE(is_exist);
    }
  }
  for (int i = 0; i < meta_block_ids.count(); i++) {
    if (meta_block_ids.at(i).is_shared_data_or_meta()) {
      FLOG_INFO("[TEST] check cold mini/minor/major not in local cache", K(meta_block_ids.at(i)));
      OK(macro_cache_mgr->exist(meta_block_ids.at(i), is_exist));
      ASSERT_FALSE(is_exist);
      tenant_file_mgr->is_exist_remote_file(meta_block_ids.at(i), 0/*ls_epoch_id*/, is_exist);
      ASSERT_TRUE(is_exist);
    }
  }
  OK(exe_sql("drop table if exists test_cold_merge"));
  FLOG_INFO("[TEST] finish test_cold_merge");
}

// 覆盖回归场景:核心内部表 __all_core_table 是 bootstrap 硬编码 schema,不走 SQL 读取路径,
// 其 simple schema 的 scp 枚举恒为 MAX_POLICY 且 full schema 策略串为空。retrive_table_policy
// 不应把这种恒常态误判为"策略未就绪"而返回 OB_SCHEMA_EAGAIN(修复前会导致每轮该表处理失败,
// first_loop_run_competed_ 永远无法置真,进而在 RESTART_RETRY_TIME_THRESHOLD 后报 -4023),
// 而应作为系统内部表被预热为 HOT
TEST_F(ObStorageCachePolicyColdTest, test_retrive_policy_for_core_inner_table)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  FLOG_INFO("[TEST] start test_retrive_policy_for_core_inner_table", K(run_ctx_));

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);

  // 取 __all_core_table 的 simple schema
  ASSERT_NE(nullptr, GCTX.schema_service_);
  share::schema::ObSchemaGetterGuard schema_guard;
  OK(GCTX.schema_service_->get_tenant_schema_guard(run_ctx_.tenant_id_, schema_guard));
  const share::schema::ObSimpleTableSchemaV2 *core_table_schema = nullptr;
  OK(schema_guard.get_simple_table_schema(
      run_ctx_.tenant_id_, OB_ALL_CORE_TABLE_TID, core_table_schema));
  ASSERT_NE(nullptr, core_table_schema);

  // 前置校验:核心表 simple schema 的 scp 枚举确为 MAX_POLICY,才能命中回退分支入口
  ASSERT_EQ(ObStorageCachePolicyType::MAX_POLICY,
      core_table_schema->get_storage_cache_policy_type());
  FLOG_INFO("[TEST] got core table simple schema",
      "table_id", core_table_schema->get_table_id(),
      "scp_type", core_table_schema->get_storage_cache_policy_type());

  // 核心动作:计算核心内部表的策略。修复前此处返回 OB_SCHEMA_EAGAIN(-5627)
  ObTablePolicyInfo table_policy_info;
  int ret = policy_service->retrive_table_policy(core_table_schema, table_policy_info);
  FLOG_INFO("[TEST] retrive_table_policy for core table finished",
      KR(ret), K(table_policy_info));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(table_policy_info.is_inited());
  // 系统内部表应被预热为 HOT
  ASSERT_TRUE(table_policy_info.get_table_policy().is_hot_policy());

  FLOG_INFO("[TEST] finish test_retrive_policy_for_core_inner_table");
}

// OnDemand 清理必须从真实 SQL 投影取得 serviceability 字段，并同时清理两级分区 policy。
TEST_F(ObStorageCachePolicyColdTest, test_clean_policy_map_for_level2_tables)
{
  using namespace share::schema;
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(tenant_id));
  ASSERT_NE(nullptr, GCTX.schema_service_);
  ASSERT_NE(nullptr, GCTX.sql_proxy_);
  ASSERT_EQ(ObSimpleSchemaMode::ON_DEMAND, GCTX.schema_service_->get_schema_mode());

  // 用合法二级分区 schema 生成测试元数据；测试 map 独立于 MTL，后台刷新不会改写断言对象。
  OK(exe_sql("create table test_clean_policy_level2_seed (c1 int, c2 int) "
      "partition by range(c1) subpartition by hash(c2) subpartitions 2 "
      "(partition p0 values less than (10), partition p1 values less than (maxvalue))"));
  DEFER({ EXPECT_EQ(OB_SUCCESS, exe_sql("drop table test_clean_policy_level2_seed")); });
  ObTableSchema seed_schema;
  {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = nullptr;
    OK(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
    OK(schema_guard.get_table_schema(tenant_id, ObString::make_string("test"),
        ObString::make_string("test_clean_policy_level2_seed"), false, table_schema));
    ASSERT_NE(nullptr, table_schema);
    ASSERT_EQ(PARTITION_LEVEL_TWO, table_schema->get_part_level());
    OK(seed_schema.assign(*table_schema));
  }

  struct TestObject
  {
    const char *name_;
    ObTableType table_type_;
    bool in_recyclebin_;
    bool hidden_;
    ObIndexStatus index_status_;
    bool keep_policy_;
  };
  const TestObject objects[] = {
      {"scp_clean_normal", USER_TABLE, false, false, INDEX_STATUS_AVAILABLE, true},
      {"scp_clean_readable_index", USER_INDEX, false, false, INDEX_STATUS_AVAILABLE, true},
      {"scp_clean_recyclebin", USER_TABLE, true, false, INDEX_STATUS_AVAILABLE, false},
      {"scp_clean_hidden", USER_TABLE, false, true, INDEX_STATUS_AVAILABLE, false},
      {"scp_clean_unreadable_index", USER_INDEX, false, false, INDEX_STATUS_UNAVAILABLE, false},
  };
  const int64_t object_count = sizeof(objects) / sizeof(objects[0]);
  const uint64_t table_id_base = 1ULL << 38;
  const int64_t part_id_base = 1LL << 40;
  const int64_t ids_per_table = 6; // 两个一级分区，每个一级分区有两个子分区。
  const int64_t part_id_end = part_id_base + object_count * ids_per_table;
  ObSqlString sql;

  // part_policy_map_ 只以 part/subpart ID 为 key，测试 ID 必须与租户其他对象完全隔离。
  OK(sql.assign_fmt(
      "SELECT (SELECT count(*) FROM %s WHERE tenant_id = 0 AND table_id >= %lu AND table_id < %lu)"
      " + (SELECT count(*) FROM %s WHERE tenant_id = 0 AND "
      "((table_id >= %lu AND table_id < %lu) OR (part_id >= %ld AND part_id < %ld)))"
      " + (SELECT count(*) FROM %s WHERE tenant_id = 0 AND "
      "((table_id >= %lu AND table_id < %lu) OR (sub_part_id >= %ld AND sub_part_id < %ld))) AS cnt",
      OB_ALL_TABLE_TNAME, table_id_base, table_id_base + object_count,
      OB_ALL_PART_TNAME, table_id_base, table_id_base + object_count, part_id_base, part_id_end,
      OB_ALL_SUB_PART_TNAME, table_id_base, table_id_base + object_count, part_id_base, part_id_end));
  {
    ObMySQLProxy::MySQLResult res;
    OK(GCTX.sql_proxy_->read(res, tenant_id, sql.ptr()));
    ASSERT_NE(nullptr, res.get_result());
    OK(res.get_result()->next());
    int64_t count = -1;
    OK(res.get_result()->get_int("cnt", count));
    ASSERT_EQ(0, count);
  }
  DEFER({
    const char *inner_tables[] = {OB_ALL_SUB_PART_TNAME, OB_ALL_PART_TNAME, OB_ALL_TABLE_TNAME};
    for (const char *inner_table : inner_tables) {
      ObSqlString cleanup_sql;
      int64_t affected_rows = 0;
      int cleanup_ret = cleanup_sql.assign_fmt(
          "DELETE FROM %s WHERE tenant_id = 0 AND table_id >= %lu AND table_id < %lu",
          inner_table, table_id_base, table_id_base + object_count);
      if (OB_SUCCESS == cleanup_ret) {
        cleanup_ret = GCTX.sql_proxy_->write(tenant_id, cleanup_sql.ptr(), affected_rows);
      }
      EXPECT_EQ(OB_SUCCESS, cleanup_ret);
    }
  });

  ObStorageCachePolicyService policy_service;
  policy_service.tenant_id_ = tenant_id;
  OK(policy_service.table_policy_map_.create(64, ObMemAttr(tenant_id, "SCPCleanTest")));
  OK(policy_service.part_policy_map_.create(64, ObMemAttr(tenant_id, "SCPCleanTest")));
  ObStorageCacheCleanTask clean_task;
  OK(clean_task.init(tenant_id));
  clean_task.policy_service_ = &policy_service;
  ObSEArray<ObTablePolicyInfo, 5> table_policies;
  ObSEArray<ObTabletPolicyInfo, 30> part_policies;
  ObStorageCachePolicy hot_policy;
  OK(hot_policy.set_global_policy(ObStorageCacheGlobalPolicy::HOT_POLICY));
  uint64_t data_version = 0;
  OK(GET_MIN_DATA_VERSION(tenant_id, data_version));

  // 只注入 STRONG lite scan 读取的 current 内表，固定不可服务状态，不等待 DDL 中间态。
  // 一个事务提交全部 table/part/subpart 行；退出用例时按专用 table ID 删除。
  ObMySQLTransaction trans;
  OK(trans.start(GCTX.sql_proxy_, tenant_id));
  DEFER({
    if (trans.is_started()) {
      EXPECT_EQ(OB_SUCCESS, trans.end(false /* rollback */));
    }
  });
  share::ObDMLExecHelper dml_helper(trans, tenant_id);
  for (int64_t i = 0; i < object_count; ++i) {
    SCOPED_TRACE(objects[i].name_);
    const uint64_t table_id = table_id_base + i;
    ObTableSchema table_schema;
    OK(table_schema.assign(seed_schema));
    table_schema.set_table_id(table_id);
    OK(table_schema.set_table_name(objects[i].name_));
    table_schema.set_table_type(objects[i].table_type_);
    table_schema.set_index_type(
        USER_INDEX == objects[i].table_type_ ? INDEX_TYPE_NORMAL_LOCAL : INDEX_TYPE_IS_NOT);
    table_schema.set_index_status(objects[i].index_status_);
    table_schema.set_data_table_id(USER_INDEX == objects[i].table_type_ ? seed_schema.get_table_id() : 0);
    table_schema.set_database_id(
        objects[i].in_recyclebin_ ? OB_RECYCLEBIN_SCHEMA_ID : seed_schema.get_database_id());
    table_schema.set_table_state_flag(objects[i].hidden_ ? TABLE_STATE_HIDDEN_OFFLINE_DDL : TABLE_STATE_NORMAL);
    share::ObDMLSqlSplicer table_dml;
    OK(ObTableSqlService::gen_table_dml_without_check(
        tenant_id, table_schema, false, data_version, table_dml));
    int64_t affected_rows = 0;
    OK(dml_helper.exec_insert(OB_ALL_TABLE_TNAME, table_dml, affected_rows));
    ASSERT_EQ(1, affected_rows);

    ObTablePolicyInfo table_policy;
    OK(table_policy.init(table_id, hot_policy, 1));
    OK(policy_service.table_policy_map_.set_refactored(table_id, table_policy));
    OK(table_policies.push_back(table_policy));
    for (int64_t p = 0; p < 2; ++p) {
      const int64_t part_id = part_id_base + i * ids_per_table + p * 3;
      OK(sql.assign_fmt(
          "INSERT INTO %s (tenant_id, table_id, part_id, schema_version, part_idx, sub_part_num, partition_type) "
          "VALUES (0, %lu, %ld, %ld, %ld, 2, %d)",
          OB_ALL_PART_TNAME, table_id, part_id, table_schema.get_schema_version(), p,
          static_cast<int>(PARTITION_TYPE_NORMAL)));
      OK(trans.write(tenant_id, sql.ptr(), affected_rows));
      ASSERT_EQ(1, affected_rows);
      ObPartition part;
      part.set_part_id(part_id);
      part.set_schema_version(1);
      part.set_part_storage_cache_policy_type(ObStorageCachePolicyType::HOT_POLICY);
      ObTabletPolicyInfo part_policy;
      OK(part_policy.init(&part));
      OK(part_policy.set_tablet_policy_status(PolicyStatus::HOT));
      OK(policy_service.part_policy_map_.set_refactored(part_id, part_policy));
      OK(part_policies.push_back(part_policy));
      for (int64_t s = 0; s < 2; ++s) {
        const int64_t subpart_id = part_id + s + 1;
        OK(sql.assign_fmt(
            "INSERT INTO %s (tenant_id, table_id, part_id, sub_part_id, schema_version, sub_part_idx, partition_type) "
            "VALUES (0, %lu, %ld, %ld, %ld, %ld, %d)",
            OB_ALL_SUB_PART_TNAME, table_id, part_id, subpart_id, table_schema.get_schema_version(), s,
            static_cast<int>(PARTITION_TYPE_NORMAL)));
        OK(trans.write(tenant_id, sql.ptr(), affected_rows));
        ASSERT_EQ(1, affected_rows);
        ObSubPartition subpart;
        subpart.set_part_id(part_id);
        subpart.set_sub_part_id(subpart_id);
        subpart.set_schema_version(1);
        subpart.set_part_storage_cache_policy_type(ObStorageCachePolicyType::HOT_POLICY);
        ObTabletPolicyInfo subpart_policy;
        OK(subpart_policy.init(&subpart));
        OK(subpart_policy.set_tablet_policy_status(PolicyStatus::HOT));
        OK(policy_service.part_policy_map_.set_refactored(subpart_id, subpart_policy));
        OK(part_policies.push_back(subpart_policy));
      }
    }
  }
  OK(trans.end(true /* commit */));
  ASSERT_EQ(object_count, policy_service.table_policy_map_.size());
  ASSERT_EQ(object_count * ids_per_table, policy_service.part_policy_map_.size());

  // 第一次清理验证删除/保留，第二次验证幂等；正常表和可读索引的内容均不能被改写。
  for (int64_t pass = 0; pass < 2; ++pass) {
    SCOPED_TRACE(pass);
    OK(clean_task.clean_policy_map_());
    ASSERT_EQ(2, policy_service.table_policy_map_.size());
    ASSERT_EQ(2 * ids_per_table, policy_service.part_policy_map_.size());
    for (int64_t i = 0; i < object_count; ++i) {
      SCOPED_TRACE(objects[i].name_);
      const int expected_ret = objects[i].keep_policy_ ? OB_SUCCESS : OB_HASH_NOT_EXIST;
      ObTablePolicyInfo actual_table_policy;
      ASSERT_EQ(expected_ret, policy_service.table_policy_map_.get_refactored(
          table_id_base + i, actual_table_policy));
      if (objects[i].keep_policy_) {
        ASSERT_TRUE(table_policies.at(i) == actual_table_policy);
      }
      for (int64_t j = 0; j < ids_per_table; ++j) {
        const ObTabletPolicyInfo &expected_policy = part_policies.at(i * ids_per_table + j);
        SCOPED_TRACE(expected_policy.get_part_id());
        ObTabletPolicyInfo actual_policy;
        ASSERT_EQ(expected_ret, policy_service.part_policy_map_.get_refactored(
            expected_policy.get_part_id(), actual_policy));
        if (objects[i].keep_policy_) {
          ASSERT_TRUE(expected_policy == actual_policy);
        }
      }
    }
  }
}

// ============================================================================
// 任务状态与不可服务对象过滤路径（5 个用例）
// ============================================================================

// ----------------------------------------------------------------------------
// 回收站、user hidden 和不可读索引暂不参与 SCP，状态恢复后重新进入检查
//
// 场景：分别构造回收站索引、user hidden table 和 UNAVAILABLE 的不可读索引。
// 操作：组合可服务状态和类型判断，再模拟 FLASHBACK、hidden swap 和 index build 完成。
// 校验：不可服务状态返回 false；恢复为 normal/AVAILABLE 并提升 schema version 后返回 true。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_check_only_queryable_table_schema)
{
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));
  static constexpr uint64_t TEST_INDEX_ID = OB_MIN_USER_OBJECT_ID + 1024;
  static constexpr uint64_t TEST_TABLE_ID = TEST_INDEX_ID + 1;
  static constexpr int64_t INITIAL_SCHEMA_VERSION = 1;
  static constexpr int64_t READY_SCHEMA_VERSION = 2;
  LOG_INFO("========== test_check_only_queryable_table_schema begin ==========",
      K(tenant_id), K(TEST_INDEX_ID));

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);

  // step 1：构造正常可读索引，确认过滤不会误伤正式对象。
  share::schema::ObSimpleTableSchemaV2 index_schema;
  index_schema.set_tenant_id(tenant_id);
  index_schema.set_table_id(TEST_INDEX_ID);
  index_schema.set_database_id(OB_SYS_DATABASE_ID);
  index_schema.set_table_type(share::schema::USER_INDEX);
  index_schema.set_index_type(share::schema::INDEX_TYPE_NORMAL_LOCAL);
  index_schema.set_index_status(share::schema::INDEX_STATUS_AVAILABLE);
  index_schema.set_schema_version(INITIAL_SCHEMA_VERSION);
  bool should_check = false;
  LOG_INFO("step 1: verify available index is checked",
      K(TEST_INDEX_ID), K(index_schema.get_index_status()));
  should_check = !policy_service->is_unserviceable_table(index_schema.get_table_type(),
      index_schema.get_table_mode_struct().mode_, index_schema.get_database_id(), index_schema.get_index_status())
      && policy_service->is_scp_candidate_table(index_schema.get_table_id(),
          index_schema.get_table_type(), index_schema.get_table_mode_struct().mode_);
  LOG_INFO("step 1 done: available index checked", K(TEST_INDEX_ID), K(should_check));
  ASSERT_TRUE(should_check);

  // step 2：回收站对象不参与 SCP；FLASHBACK 提升 schema version 后重新进入检查。
  index_schema.set_database_id(OB_RECYCLEBIN_SCHEMA_ID);
  should_check = true;
  LOG_INFO("step 2: verify recyclebin index is skipped", K(TEST_INDEX_ID));
  should_check = !policy_service->is_unserviceable_table(index_schema.get_table_type(),
      index_schema.get_table_mode_struct().mode_, index_schema.get_database_id(), index_schema.get_index_status())
      && policy_service->is_scp_candidate_table(index_schema.get_table_id(),
          index_schema.get_table_type(), index_schema.get_table_mode_struct().mode_);
  LOG_INFO("step 2 done: recyclebin index skipped", K(TEST_INDEX_ID), K(should_check));
  ASSERT_FALSE(should_check);
  index_schema.set_database_id(OB_SYS_DATABASE_ID);
  index_schema.set_schema_version(READY_SCHEMA_VERSION);
  should_check = !policy_service->is_unserviceable_table(index_schema.get_table_type(),
      index_schema.get_table_mode_struct().mode_, index_schema.get_database_id(), index_schema.get_index_status())
      && policy_service->is_scp_candidate_table(index_schema.get_table_id(),
          index_schema.get_table_type(), index_schema.get_table_mode_struct().mode_);
  LOG_INFO("step 2 done: flashback index checked again",
      K(TEST_INDEX_ID), K(index_schema.get_schema_version()), K(should_check));
  ASSERT_TRUE(should_check);

  // step 3：user hidden table 延迟预热；swap 为 normal 并提升版本后重新进入检查。
  share::schema::ObSimpleTableSchemaV2 hidden_table_schema;
  hidden_table_schema.set_tenant_id(tenant_id);
  hidden_table_schema.set_table_id(TEST_TABLE_ID);
  hidden_table_schema.set_database_id(OB_SYS_DATABASE_ID);
  hidden_table_schema.set_table_type(share::schema::USER_TABLE);
  hidden_table_schema.set_table_state_flag(share::schema::TABLE_STATE_HIDDEN_OFFLINE_DDL);
  hidden_table_schema.set_schema_version(INITIAL_SCHEMA_VERSION);
  should_check = true;
  LOG_INFO("step 3: verify user hidden table is skipped", K(TEST_TABLE_ID));
  should_check = !policy_service->is_unserviceable_table(hidden_table_schema.get_table_type(),
      hidden_table_schema.get_table_mode_struct().mode_, hidden_table_schema.get_database_id(),
      hidden_table_schema.get_index_status())
      && policy_service->is_scp_candidate_table(hidden_table_schema.get_table_id(),
          hidden_table_schema.get_table_type(), hidden_table_schema.get_table_mode_struct().mode_);
  LOG_INFO("step 3 done: user hidden table skipped", K(TEST_TABLE_ID), K(should_check));
  ASSERT_FALSE(should_check);
  hidden_table_schema.set_table_state_flag(share::schema::TABLE_STATE_NORMAL);
  hidden_table_schema.set_schema_version(READY_SCHEMA_VERSION);
  should_check = !policy_service->is_unserviceable_table(hidden_table_schema.get_table_type(),
      hidden_table_schema.get_table_mode_struct().mode_, hidden_table_schema.get_database_id(),
      hidden_table_schema.get_index_status())
      && policy_service->is_scp_candidate_table(hidden_table_schema.get_table_id(),
          hidden_table_schema.get_table_type(), hidden_table_schema.get_table_mode_struct().mode_);
  LOG_INFO("step 3 done: normal table checked after swap",
      K(TEST_TABLE_ID), K(hidden_table_schema.get_schema_version()), K(should_check));
  ASSERT_TRUE(should_check);

  // step 4：不可读索引暂不预热；building index 转为 AVAILABLE 并提升版本后重新进入检查。
  index_schema.set_index_status(share::schema::INDEX_STATUS_UNAVAILABLE);
  index_schema.set_schema_version(INITIAL_SCHEMA_VERSION);
  should_check = true;
  LOG_INFO("step 4: verify building index is skipped",
      K(TEST_INDEX_ID), K(index_schema.get_index_status()));
  should_check = !policy_service->is_unserviceable_table(index_schema.get_table_type(),
      index_schema.get_table_mode_struct().mode_, index_schema.get_database_id(), index_schema.get_index_status())
      && policy_service->is_scp_candidate_table(index_schema.get_table_id(),
          index_schema.get_table_type(), index_schema.get_table_mode_struct().mode_);
  LOG_INFO("step 4 done: building index skipped", K(TEST_INDEX_ID), K(should_check));
  ASSERT_FALSE(should_check);
  index_schema.set_index_status(share::schema::INDEX_STATUS_AVAILABLE);
  index_schema.set_schema_version(READY_SCHEMA_VERSION);
  should_check = !policy_service->is_unserviceable_table(index_schema.get_table_type(),
      index_schema.get_table_mode_struct().mode_, index_schema.get_database_id(), index_schema.get_index_status())
      && policy_service->is_scp_candidate_table(index_schema.get_table_id(),
          index_schema.get_table_type(), index_schema.get_table_mode_struct().mode_);
  LOG_INFO("step 4 done: available index checked after build",
      K(TEST_INDEX_ID), K(index_schema.get_schema_version()), K(should_check));
  ASSERT_TRUE(should_check);

  LOG_INFO("========== test_check_only_queryable_table_schema end ==========",
      K(tenant_id), K(TEST_INDEX_ID), K(TEST_TABLE_ID));
}

// ----------------------------------------------------------------------------
// 不可服务对象会把所有状态的历史任务强制置为 CANCELED，阻止后续 regenerate
//
// 场景：分别构造 INIT、DOING、SUSPENDED、FINISHED 和 FAILED 的预热任务。
// 操作：每个任务均写入 tablet_task_map_，再调用 invalidate_task_by_tablet_id。
// 校验：scheduler 取回同一任务并将其强制转为 CANCELED，不再满足 resume/skipped 重生条件。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_invalidate_task_for_unserviceable_schema)
{
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));
  static constexpr int64_t BASE_TABLET_ID =
      common::ObTabletID::MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID - 1032;
  static constexpr ObStorageCacheTaskStatusType TASK_STATUSES[] = {
      ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT,
      ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING,
      ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED,
      ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED,
      ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FAILED};
  static constexpr int64_t TASK_STATUS_COUNT = ARRAYSIZEOF(TASK_STATUSES);
  LOG_INFO("========== test_invalidate_task_for_unserviceable_schema begin ==========",
      K(tenant_id), K(BASE_TABLET_ID), K(TASK_STATUS_COUNT));

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);

  for (int64_t i = 0; i < TASK_STATUS_COUNT; ++i) {
    const int64_t tablet_id = BASE_TABLET_ID + i;
    const ObStorageCacheTaskStatusType task_status = TASK_STATUSES[i];
    LOG_INFO("test task invalidation through scheduler", K(i), K(tablet_id), K(task_status));

    // 构造目标状态任务并写入生产 tablet task map。
    ObStorageCacheTabletTask *task = OB_NEW(
        ObStorageCacheTabletTask, ObMemAttr(tenant_id, "SCPTestCancel"));
    ASSERT_NE(nullptr, task);
    ObStorageCacheTabletTaskHandle task_handle;
    task_handle.set_ptr(task);
    OK(task->init_partition_task(tenant_id, tablet_id, PolicyStatus::HOT));
    if (ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT == task_status) {
      // do nothing
    } else if (ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED == task_status
        || ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FAILED == task_status) {
      OK(task->set_status(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING));
      OK(task->set_status(task_status));
    } else {
      OK(task->set_status(task_status));
    }
    OK(policy_service->tablet_scheduler_.tablet_task_map_.set_refactored(tablet_id, task_handle));

    // 通过 scheduler 生产接口失效任务，并确认 map 中同一任务被强制取消。
    OK(policy_service->tablet_scheduler_.invalidate_task_by_tablet_id(tablet_id));
    ObStorageCacheTabletTaskHandle stored_task_handle;
    OK(policy_service->tablet_scheduler_.tablet_task_map_.get_refactored(
        tablet_id, stored_task_handle));
    ASSERT_TRUE(stored_task_handle.is_valid());
    ASSERT_EQ(task, stored_task_handle.get_ptr());
    const ObStorageCacheTaskStatusType canceled_status = stored_task_handle()->get_status();
    LOG_INFO("task canceled through scheduler",
        K(i), K(tablet_id), K(task_status), K(canceled_status), KPC(task));
    ASSERT_EQ(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED, canceled_status);
    ASSERT_TRUE(stored_task_handle()->is_canceled());

    // 每轮立即清理，避免影响同 fixture 的后续用例。
    OK(policy_service->tablet_scheduler_.tablet_task_map_.erase_refactored(tablet_id));
    stored_task_handle.reset();
    task_handle.reset();
  }

  LOG_INFO("========== test_invalidate_task_for_unserviceable_schema end ==========",
      K(tenant_id), K(BASE_TABLET_ID));
}

// ----------------------------------------------------------------------------
// OB_EAGAIN 调度失败等待重试，其他调度失败保持最终失败
//
// 场景：分别使用 OB_EAGAIN 和 OB_ALLOCATE_MEMORY_FAILED 记录调度失败。
// 操作：检查任务状态、结果码和 retry 扫描结果，再触发 inactive tablet 的 retry 扫描。
// 校验：OB_EAGAIN 任务保持 INIT 并记录错误码，其他错误为 FAILED；retry 扫描
//       只返回 INIT + OB_EAGAIN，且 inactive tablet 的待重试任务会被取消。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_schedule_failure_status_and_retry_scan)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));
  static constexpr int64_t RETRY_TABLET_ID =
      common::ObTabletID::MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID - 1022;
  static constexpr int64_t FAILED_TABLET_ID = RETRY_TABLET_ID + 1;
  LOG_INFO("========== test_schedule_failure_status_and_retry_scan begin ==========",
      K(tenant_id), K(RETRY_TABLET_ID), K(FAILED_TABLET_ID));

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  bool retry_task_inserted = false;
  bool failed_task_inserted = false;
  DEFER({
    if (retry_task_inserted) {
      const int cleanup_ret = policy_service->tablet_scheduler_.tablet_task_map_.erase_refactored(
          RETRY_TABLET_ID);
      if (OB_SUCCESS != cleanup_ret && OB_HASH_NOT_EXIST != cleanup_ret) {
        LOG_ERROR("fail to clean retry waiting test task",
            KR(cleanup_ret), K(RETRY_TABLET_ID), K(tenant_id));
      }
    }
    if (failed_task_inserted) {
      const int cleanup_ret = policy_service->tablet_scheduler_.tablet_task_map_.erase_refactored(
          FAILED_TABLET_ID);
      if (OB_SUCCESS != cleanup_ret && OB_HASH_NOT_EXIST != cleanup_ret) {
        LOG_ERROR("fail to clean failed test task",
            KR(cleanup_ret), K(FAILED_TABLET_ID), K(tenant_id));
      }
    }
  });

  // step 1：记录 OB_EAGAIN，确认状态和错误码可由虚拟表同源 getter 读取。
  LOG_INFO("step 1: prepare retry waiting task", K(RETRY_TABLET_ID));
  ObStorageCacheTabletTask *retry_task = OB_NEW(
      ObStorageCacheTabletTask, ObMemAttr(tenant_id, "SCPTestRetry"));
  ASSERT_NE(nullptr, retry_task);
  ObStorageCacheTabletTaskHandle retry_task_handle;
  retry_task_handle.set_ptr(retry_task);
  OK(retry_task->init_partition_task(tenant_id, RETRY_TABLET_ID, PolicyStatus::HOT));
  OK(retry_task->mark_schedule_failure(OB_EAGAIN));
  const ObStorageCacheTaskStatusType retry_status = retry_task->get_status();
  const int retry_result = retry_task->get_result();
  LOG_INFO("step 1 done: retry waiting task prepared",
      K(RETRY_TABLET_ID), K(retry_status), K(retry_result));
  ASSERT_EQ(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT, retry_status);
  ASSERT_EQ(OB_EAGAIN, retry_result);
  ASSERT_STREQ("WAITING", ObStorageCacheTaskStatus::get_str(retry_status));
  ASSERT_TRUE(retry_task->need_retry_push_task());
  ASSERT_FALSE(retry_task->is_completed());
  ASSERT_TRUE(retry_task->is_uncompleted());
  OK(policy_service->tablet_scheduler_.tablet_task_map_.set_refactored(
      RETRY_TABLET_ID, retry_task_handle));
  retry_task_inserted = true;

  // step 2：非 OB_EAGAIN 调度错误必须使用最终 FAILED 语义。
  LOG_INFO("step 2: prepare terminal scheduling failure", K(FAILED_TABLET_ID));
  ObStorageCacheTabletTask *failed_task = OB_NEW(
      ObStorageCacheTabletTask, ObMemAttr(tenant_id, "SCPTestFailed"));
  ASSERT_NE(nullptr, failed_task);
  ObStorageCacheTabletTaskHandle failed_task_handle;
  failed_task_handle.set_ptr(failed_task);
  OK(failed_task->init_partition_task(tenant_id, FAILED_TABLET_ID, PolicyStatus::HOT));
  OK(failed_task->mark_schedule_failure(OB_ALLOCATE_MEMORY_FAILED));
  const ObStorageCacheTaskStatusType failed_status = failed_task->get_status();
  const int failed_result = failed_task->get_result();
  const int64_t failed_end_time_us = failed_task->get_end_time();
  LOG_INFO("step 2 done: terminal scheduling failure prepared",
      K(FAILED_TABLET_ID), K(failed_status), K(failed_result), K(failed_end_time_us));
  ASSERT_EQ(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FAILED, failed_status);
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, failed_result);
  ASSERT_GT(failed_end_time_us, 0);
  ASSERT_FALSE(failed_task->need_retry_push_task());
  ASSERT_TRUE(failed_task->is_completed());
  OK(policy_service->tablet_scheduler_.tablet_task_map_.set_refactored(
      FAILED_TABLET_ID, failed_task_handle));
  failed_task_inserted = true;

  // step 3：扫描 map，确认仅队列满任务被识别为待重试任务。
  LOG_INFO("step 3: scan retry waiting tablet ids");
  ObSEArray<uint64_t, 8> retry_tablet_ids;
  retry_tablet_ids.set_attr(ObMemAttr(tenant_id, "SCPTestRetry"));
  OK(policy_service->tablet_scheduler_.get_retry_waiting_tablet_ids(retry_tablet_ids));
  bool found_retry_tablet = false;
  bool found_failed_tablet = false;
  for (int64_t i = 0; i < retry_tablet_ids.count(); ++i) {
    found_retry_tablet |= RETRY_TABLET_ID == retry_tablet_ids.at(i);
    found_failed_tablet |= FAILED_TABLET_ID == retry_tablet_ids.at(i);
  }
  LOG_INFO("step 3 done: retry waiting tasks scanned",
      K(retry_tablet_ids), K(found_retry_tablet), K(found_failed_tablet));
  ASSERT_TRUE(found_retry_tablet);
  ASSERT_FALSE(found_failed_tablet);

  // step 4：合成 tablet 不在 active 集合中，retry 扫描应取消它并清除重试信号。
  bool is_active = true;
  OK(policy_service->tablet_scheduler_.check_is_active_tablet(RETRY_TABLET_ID, is_active));
  LOG_INFO("step 4: verify retry waiting tablet is inactive", K(RETRY_TABLET_ID), K(is_active));
  ASSERT_FALSE(is_active);
  int64_t queue_task_count = -1;
  OK(TG_GET_QUEUE_NUM(policy_service->tablet_scheduler_.tg_id_, queue_task_count));
  LOG_INFO("step 4: verify storage cache task queue has no pending task", K(queue_task_count));
  ASSERT_LE(queue_task_count, 0);
  LOG_INFO("step 4: trigger retry scan for inactive tablet", K(RETRY_TABLET_ID));
  OK(policy_service->tablet_scheduler_.trigger_retry_waiting_tasks());
  const ObStorageCacheTaskStatusType canceled_status = retry_task->get_status();
  const bool has_retry_waiting_task = policy_service->tablet_scheduler_.has_retry_waiting_task();
  LOG_INFO("step 4 done: inactive retry waiting task canceled",
      K(RETRY_TABLET_ID), K(canceled_status), K(has_retry_waiting_task));
  ASSERT_EQ(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED, canceled_status);
  ASSERT_FALSE(has_retry_waiting_task);
  OK(policy_service->tablet_scheduler_.tablet_task_map_.erase_refactored(RETRY_TABLET_ID));
  retry_task_inserted = false;
  OK(policy_service->tablet_scheduler_.tablet_task_map_.erase_refactored(FAILED_TABLET_ID));
  failed_task_inserted = false;
  retry_task_handle.reset();
  failed_task_handle.reset();
  LOG_INFO("========== test_schedule_failure_status_and_retry_scan end ==========",
      K(tenant_id), K(RETRY_TABLET_ID), K(FAILED_TABLET_ID));
}

// ----------------------------------------------------------------------------
// HOT 入队失败保持 WAITING，显式 AUTO 预热和周期 refresh 均按 HOT 恢复
//
// 场景：构造真实 HOT tablet，并在 TG_PUSH_TASK 前通过 EN_PUSH_TASK_FAILED
//       连续模拟两次 OB_EAGAIN。
// 操作：首次生成 HOT task 并注入 OB_EAGAIN；显式 AUTO 预热立即重试并再次失败；
//       最后在 TG queue 为空时执行普通 refresh。
// 校验：失败任务均为 INIT + OB_EAGAIN；显式 AUTO 请求不得将 HOT 降级为 AUTO；
//       周期 refresh 最终生成新的 HOT task，并由 TG worker 执行至 FINISHED。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_push_eagain_waiting_and_refresh_regenerate)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));
  LOG_INFO("========== test_push_eagain_waiting_and_refresh_regenerate begin ==========",
      K(tenant_id));

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObStorageCacheTabletScheduler &tablet_scheduler = policy_service->tablet_scheduler_;
  SCPTabletTaskMap &tablet_task_map = policy_service->get_tablet_tasks();

  // 暂停周期 refresh，避免它在校验 INIT + OB_EAGAIN 前自动重试；退出 case 时恢复定时任务。
  bool need_restore_refresh_task = false;
  DEFER({
    if (need_restore_refresh_task) {
      const int cleanup_ret = TG_SCHEDULE(policy_service->refresh_policy_scheduler_.tg_id_,
          policy_service->refresh_policy_scheduler_.refresh_policy_task_, 30_s, true);
      if (OB_SUCCESS != cleanup_ret) {
        LOG_ERROR("fail to restore storage cache refresh task", KR(cleanup_ret), K(tenant_id));
      }
    }
  });
  TG_CANCEL_TASK(policy_service->refresh_policy_scheduler_.tg_id_,
      policy_service->refresh_policy_scheduler_.refresh_policy_task_);
  TG_WAIT_TASK(policy_service->refresh_policy_scheduler_.tg_id_,
      policy_service->refresh_policy_scheduler_.refresh_policy_task_);
  need_restore_refresh_task = true;

  // 构造真实 HOT tablet，使普通 refresh 中的 active tablet 检查能够通过。
  static constexpr char TEST_TABLE_NAME[] = "test_queue_full_retry_refresh";
  bool need_drop_table = false;
  DEFER({
    if (need_drop_table) {
      const int cleanup_ret = exe_sql("drop table if exists test_queue_full_retry_refresh");
      if (OB_SUCCESS != cleanup_ret) {
        LOG_ERROR("fail to drop queue full retry test table", KR(cleanup_ret), K(tenant_id));
      }
    }
  });
  OK(exe_sql("drop table if exists test_queue_full_retry_refresh"));
  need_drop_table = true;
  OK(exe_sql("create table test_queue_full_retry_refresh (c1 int)"));
  set_ls_and_tablet_id_for_run_ctx(TEST_TABLE_NAME);
  const int64_t tablet_id = run_ctx_.tablet_id_.id();
  bool task_may_exist = true;
  DEFER({
    if (task_may_exist) {
      int cleanup_ret = tablet_scheduler.invalidate_task_by_tablet_id(tablet_id);
      if (OB_SUCCESS != cleanup_ret && OB_HASH_NOT_EXIST != cleanup_ret) {
        LOG_ERROR("fail to cancel queue full retry test task",
            KR(cleanup_ret), K(tablet_id), K(tenant_id));
      }
      cleanup_ret = tablet_task_map.erase_refactored(tablet_id);
      if (OB_SUCCESS != cleanup_ret && OB_HASH_NOT_EXIST != cleanup_ret) {
        LOG_ERROR("fail to clean queue full retry test task",
            KR(cleanup_ret), K(tablet_id), K(tenant_id));
      }
    }
  });
  LOG_INFO("step 1: prepare real hot tablet", K(tenant_id), K(tablet_id));
  OK(exe_sql("insert into test_queue_full_retry_refresh values (1)"));
  OK(exe_sql("insert into test_queue_full_retry_refresh values (2)"));
  OK(TestCompactionUtil::medium_compact(tenant_id, tablet_id, run_ctx_.ls_id_));
  OK(exe_sql("alter table test_queue_full_retry_refresh storage_cache_policy (global = 'hot')"));
  OK(policy_service->trigger_or_refresh_tablet(
      ObStorageCachePolicyRefreshType::REFRESH_TYPE_FORCE_REFRESH));
  wait_task_finished(tablet_id);
  LOG_INFO("step 1 done: real hot tablet prepared", K(tenant_id), K(tablet_id));

  // 注入一次 push OB_EAGAIN，退出 case 时始终关闭故障点，避免影响其他用例。
  DEFER({
    TP_SET_EVENT(common::EventTable::EN_PUSH_TASK_FAILED, OB_SUCCESS, 0, 0);
  });
  TP_SET_EVENT(common::EventTable::EN_PUSH_TASK_FAILED, OB_EAGAIN, 1, 0);
  LOG_INFO("step 2: inject one push failure", K(tenant_id), K(tablet_id));
  OK(tablet_scheduler.generate_partition_task(tenant_id, tablet_id, PolicyStatus::HOT));
  TP_SET_EVENT(common::EventTable::EN_PUSH_TASK_FAILED, OB_SUCCESS, 0, 0);
  ObStorageCacheTabletTaskHandle retry_task_handle;
  OK(tablet_task_map.get_refactored(tablet_id, retry_task_handle));
  ASSERT_TRUE(retry_task_handle.is_valid());
  ObStorageCacheTabletTask *retry_task = retry_task_handle.get_ptr();
  const ObStorageCacheTaskStatusType retry_status = retry_task->get_status();
  const int retry_result = retry_task->get_result();
  LOG_INFO("step 2 done: verify push failure task waits for retry",
      K(tablet_id), K(retry_status), K(retry_result), KPC(retry_task));
  ASSERT_EQ(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT, retry_status);
  ASSERT_EQ(OB_EAGAIN, retry_result);
  ASSERT_STREQ("WAITING", ObStorageCacheTaskStatus::get_str(retry_status));
  ASSERT_TRUE(retry_task->need_retry_push_task());
  ASSERT_TRUE(tablet_scheduler.has_retry_waiting_task());

  // 显式 AUTO 预热应立即重试，但必须保留已有 HOT 待重试任务的更强语义。
  TP_SET_EVENT(common::EventTable::EN_PUSH_TASK_FAILED, OB_EAGAIN, 1, 0);
  LOG_INFO("step 3: retry hot task by explicit auto prewarm request", K(tablet_id));
  OK(policy_service->async_prewarm_tablet(tablet_id, PolicyStatus::AUTO));
  TP_SET_EVENT(common::EventTable::EN_PUSH_TASK_FAILED, OB_SUCCESS, 0, 0);
  ObStorageCacheTabletTaskHandle second_retry_task_handle;
  OK(tablet_task_map.get_refactored(tablet_id, second_retry_task_handle));
  ASSERT_TRUE(second_retry_task_handle.is_valid());
  ObStorageCacheTabletTask *second_retry_task = second_retry_task_handle.get_ptr();
  const ObStorageCacheTaskStatusType second_retry_status = second_retry_task->get_status();
  const int second_retry_result = second_retry_task->get_result();
  LOG_INFO("step 3 done: explicit auto request retries with hot policy",
      K(tablet_id), K(second_retry_status), K(second_retry_result),
      KPC(retry_task), KPC(second_retry_task));
  ASSERT_NE(retry_task, second_retry_task);
  ASSERT_EQ(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT, second_retry_status);
  ASSERT_EQ(PolicyStatus::HOT, second_retry_task->get_policy_status());
  ASSERT_EQ(OB_EAGAIN, second_retry_result);
  ASSERT_TRUE(second_retry_task->need_retry_push_task());
  ASSERT_TRUE(tablet_scheduler.has_retry_waiting_task());

  // TG queue 为空时周期 refresh 扫描待重试 task，并通过真实 TG push 入队。
  int64_t queue_task_count = -1;
  OK(TG_GET_QUEUE_NUM(tablet_scheduler.tg_id_, queue_task_count));
  LOG_INFO("step 4: verify storage cache task queue has no pending task", K(queue_task_count));
  ASSERT_LE(queue_task_count, 0);
  LOG_INFO("step 4: trigger normal refresh for enqueue retry", K(tablet_id));
  OK(policy_service->trigger_or_refresh_tablet(
      ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  ObStorageCacheTabletTaskHandle regenerated_task_handle;
  OK(tablet_task_map.get_refactored(tablet_id, regenerated_task_handle));
  ASSERT_TRUE(regenerated_task_handle.is_valid());
  ObStorageCacheTabletTask *regenerated_task = regenerated_task_handle.get_ptr();
  const ObStorageCacheTaskStatusType second_retry_status_after_refresh =
      second_retry_task->get_status();
  LOG_INFO("step 4 done: enqueue retry task regenerated and enqueued",
      K(tablet_id), K(second_retry_status_after_refresh),
      KPC(second_retry_task), KPC(regenerated_task));
  ASSERT_NE(second_retry_task, regenerated_task);
  ASSERT_EQ(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT,
      second_retry_status_after_refresh);
  ASSERT_EQ(OB_EAGAIN, second_retry_task->get_result());
  ASSERT_EQ(PolicyStatus::HOT, regenerated_task->get_policy_status());
  ASSERT_FALSE(regenerated_task->need_retry_push_task());
  ASSERT_FALSE(tablet_scheduler.has_retry_waiting_task());

  // 等待新任务完成，确认重生成任务已被真实 TG worker 消费。
  LOG_INFO("step 5: wait regenerated task finished", K(tablet_id));
  wait_task_finished(tablet_id);
  ObStorageCacheTabletTaskHandle finished_task_handle;
  OK(tablet_task_map.get_refactored(tablet_id, finished_task_handle));
  ASSERT_TRUE(finished_task_handle.is_valid());
  const ObStorageCacheTaskStatusType finished_status = finished_task_handle()->get_status();
  LOG_INFO("step 5 done: regenerated task finished",
      K(tablet_id), K(finished_status), KPC(finished_task_handle.get_ptr()));
  ASSERT_EQ(regenerated_task, finished_task_handle.get_ptr());
  ASSERT_EQ(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED, finished_status);
  OK(tablet_task_map.erase_refactored(tablet_id));
  task_may_exist = false;

  LOG_INFO("========== test_push_eagain_waiting_and_refresh_regenerate end ==========",
      K(tenant_id), K(tablet_id));
}

// ----------------------------------------------------------------------------
// 不可服务表失效历史任务并清理旧策略，恢复后普通刷新会立即重建策略
//
// 场景：构造含单个 tablet 的 simple schema，并写入 task、tablet status 和 table policy 旧状态。
// 操作：调用 invalidate_tablet_tasks_ 后，将对象恢复为可服务状态并执行普通策略检查。
// 校验：旧任务转为 CANCELED，tablet/table policy 均被删除；恢复后必须要求重算。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_invalidate_tablet_tasks_from_schema)
{
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));
  static constexpr uint64_t TEST_TABLE_ID = OB_MIN_USER_OBJECT_ID + 2048;
  static constexpr int64_t TEST_TABLET_ID =
      common::ObTabletID::MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID - 1024;
  static constexpr int64_t STALE_SCHEMA_VERSION = 1;
  static constexpr int64_t READY_SCHEMA_VERSION = 2;
  LOG_INFO("========== test_invalidate_tablet_tasks_from_schema begin ==========",
      K(tenant_id), K(TEST_TABLE_ID), K(TEST_TABLET_ID));

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);

  // step 1：构造可由 get_tablet_ids 直接取得 tablet id 的非分区表 schema。
  share::schema::ObSimpleTableSchemaV2 table_schema;
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_table_id(TEST_TABLE_ID);
  table_schema.set_database_id(OB_RECYCLEBIN_SCHEMA_ID);
  table_schema.set_table_type(share::schema::USER_TABLE);
  table_schema.set_part_level(share::schema::PARTITION_LEVEL_ZERO);
  table_schema.set_tablet_id(common::ObTabletID(TEST_TABLET_ID));
  table_schema.set_schema_version(STALE_SCHEMA_VERSION);
  table_schema.set_storage_cache_policy_type(ObStorageCachePolicyType::HOT_POLICY);
  ObSEArray<common::ObTabletID, 1> tablet_ids;
  tablet_ids.set_attr(ObMemAttr(tenant_id, "SCPTestInvalid"));
  OK(table_schema.get_tablet_ids(tablet_ids));
  LOG_INFO("step 1 done: tablet ids prepared", K(TEST_TABLE_ID), K(TEST_TABLET_ID), K(tablet_ids));
  ASSERT_EQ(1, tablet_ids.count());
  ASSERT_EQ(TEST_TABLET_ID, tablet_ids.at(0).id());

  // step 2：向 task、tablet status 和 table policy map 写入旧预热状态。
  ObStorageCacheTabletTask *task = OB_NEW(
      ObStorageCacheTabletTask, ObMemAttr(tenant_id, "SCPTestTask"));
  ASSERT_NE(nullptr, task);
  ObStorageCacheTabletTaskHandle task_handle;
  task_handle.set_ptr(task);
  OK(task->init_partition_task(tenant_id, TEST_TABLET_ID, PolicyStatus::HOT));
  OK(task->set_status(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED));
  OK(policy_service->tablet_scheduler_.tablet_task_map_.set_refactored(TEST_TABLET_ID, task_handle));
  OK(policy_service->tablet_status_map_.set_refactored(TEST_TABLET_ID, PolicyStatus::HOT));
  ObStorageCachePolicy stale_table_policy;
  OK(stale_table_policy.set_global_policy(ObStorageCacheGlobalPolicy::HOT_POLICY));
  ObTablePolicyInfo stale_table_policy_info;
  OK(stale_table_policy_info.init(TEST_TABLE_ID, stale_table_policy, STALE_SCHEMA_VERSION));
  OK(policy_service->table_policy_map_.set_refactored(TEST_TABLE_ID, stale_table_policy_info));
  LOG_INFO("step 2 done: stale task, tablet status and table policy inserted",
      K(TEST_TABLE_ID), K(TEST_TABLET_ID), KPC(task));

  // step 3：执行表级取消和清理，验证任务被取消且旧 policy 均被删除。
  OK(policy_service->invalidate_tablet_tasks_(&table_schema));
  ObStorageCacheTabletTaskHandle stored_task_handle;
  OK(policy_service->tablet_scheduler_.tablet_task_map_.get_refactored(
      TEST_TABLET_ID, stored_task_handle));
  ASSERT_TRUE(stored_task_handle.is_valid());
  const ObStorageCacheTaskStatusType stored_status = stored_task_handle()->get_status();
  LOG_INFO("step 3: verify task canceled and stale policies erased",
      K(TEST_TABLE_ID), K(TEST_TABLET_ID), K(stored_status));
  ASSERT_EQ(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED, stored_status);
  PolicyStatus stale_policy_status = PolicyStatus::MAX_STATUS;
  ASSERT_EQ(OB_HASH_NOT_EXIST,
      policy_service->tablet_status_map_.get_refactored(TEST_TABLET_ID, stale_policy_status));
  ObTablePolicyInfo erased_table_policy_info;
  ASSERT_EQ(OB_HASH_NOT_EXIST,
      policy_service->table_policy_map_.get_refactored(TEST_TABLE_ID, erased_table_policy_info));

  // step 4：对象恢复可服务后，普通刷新必须因 table policy 缺失而强制重算。
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_schema_version(READY_SCHEMA_VERSION);
  bool table_policy_need_calc = false;
  OK(policy_service->check_table_policy_need_recaculate(
      &table_schema, ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL, table_policy_need_calc));
  LOG_INFO("step 4 done: recovered table requires policy recalculation",
      K(TEST_TABLE_ID), K(TEST_TABLET_ID), K(table_policy_need_calc));
  ASSERT_TRUE(table_policy_need_calc);

  // step 5：清理测试注入和恢复时重建的记录，避免影响同 fixture 的后续用例。
  OK(policy_service->tablet_scheduler_.tablet_task_map_.erase_refactored(TEST_TABLET_ID));
  OK(policy_service->table_policy_map_.erase_refactored(TEST_TABLE_ID));
  stored_task_handle.reset();
  task_handle.reset();
  LOG_INFO("========== test_invalidate_tablet_tasks_from_schema end ==========",
      K(tenant_id), K(TEST_TABLE_ID), K(TEST_TABLET_ID));
}

// ============================================================================
// BLOCK 存储缓存策略禁用路径（2 个用例）
// ============================================================================

// ----------------------------------------------------------------------------
// schema 持久化前必须拒绝 BLOCK 策略，且不能误伤合法的 GLOBAL 策略
//
// 场景：通过正常 SQL 创建一张 GLOBAL AUTO 表，再复制其完整 schema。
// 操作：直接把 schema 副本改为 BLOCK，绕过 SQL resolver 调用 check_table_options；
//       随后把同一 schema 改回 GLOBAL AUTO 作为对照。
// 校验：BLOCK 返回 OB_NOT_SUPPORTED；GLOBAL AUTO 返回 OB_SUCCESS。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_reject_block_policy_before_schema_persistence)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  static const char *TABLE_NAME = "test_reject_block_policy_before_schema_persistence";
  static const char *BLOCK_POLICY =
      R"({"GRANULARITY":"BLOCK","HOT_RETENTION_INTERVAL":1,"HOT_RETENTION_UNIT":"DAY"})";
  LOG_INFO("========== test_reject_block_policy_before_schema_persistence begin ==========",
      K(run_ctx_.tenant_id_), K(TABLE_NAME));

  // step 1：创建合法表并取得完整 schema，确保后续只改变被测策略字段。
  LOG_INFO("step 1: create table and fetch full schema", K(TABLE_NAME));
  OK(exe_sql("drop table if exists test_reject_block_policy_before_schema_persistence"));
  OK(exe_sql("create table test_reject_block_policy_before_schema_persistence (c1 int primary key)"));

  ASSERT_NE(nullptr, GCTX.schema_service_);
  share::schema::ObSchemaGetterGuard schema_guard;
  OK(GCTX.schema_service_->get_tenant_schema_guard(run_ctx_.tenant_id_, schema_guard));
  const share::schema::ObTableSchema *table_schema = nullptr;
  OK(schema_guard.get_table_schema(
      run_ctx_.tenant_id_, common::ObString::make_string("test"), common::ObString::make_string(TABLE_NAME),
      false /* is_index */, table_schema));
  ASSERT_NE(nullptr, table_schema);
  LOG_INFO("step 1 done: full schema fetched",
      K(table_schema->get_table_id()), K(table_schema->get_storage_cache_policy()));

  // step 2：直接注入 BLOCK 字符串，验证 schema 落库兜底不依赖 resolver。
  share::schema::ObTableSchema test_schema;
  OK(test_schema.assign(*table_schema));
  OK(test_schema.set_storage_cache_policy(BLOCK_POLICY));
  LOG_INFO("step 2: verify BLOCK policy is rejected",
      K(test_schema.get_table_id()), K(test_schema.get_storage_cache_policy()));
  int ret = share::schema::ObTableSqlService::check_table_options(test_schema);
  LOG_INFO("step 2 done: BLOCK policy checked", KR(ret), K(test_schema.get_table_id()));
  ASSERT_EQ(OB_NOT_SUPPORTED, ret);

  // step 3：改回 GLOBAL AUTO，确认新增 guard 不影响合法策略。
  OK(test_schema.set_storage_cache_policy(storage::OB_DEFAULT_STORAGE_CACHE_POLICY_STR));
  LOG_INFO("step 3: verify GLOBAL AUTO policy is accepted",
      K(test_schema.get_table_id()), K(test_schema.get_storage_cache_policy()));
  ret = share::schema::ObTableSqlService::check_table_options(test_schema);
  LOG_INFO("step 3 done: GLOBAL AUTO policy checked", KR(ret), K(test_schema.get_table_id()));
  OK(ret);

  // step 4：清理测试表，避免影响同文件后续 case。
  OK(exe_sql("drop table test_reject_block_policy_before_schema_persistence"));
  LOG_INFO("========== test_reject_block_policy_before_schema_persistence end ==========",
      K(run_ctx_.tenant_id_), K(TABLE_NAME));
}

// ----------------------------------------------------------------------------
// runtime 加载存量 BLOCK schema 时必须失败，且不能生成有效的 table policy
//
// 场景：手工构造 TIME simple schema 和带 BLOCK JSON 的 full schema。
// 操作：通过隔离的 mock schema service 注入 full schema，直接调用 retrive_table_policy，
//       完全绕过 SQL resolver 和 schema 持久化 guard。
// 校验：返回 OB_NOT_SUPPORTED，输出的 ObTablePolicyInfo 保持未初始化状态。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_retrive_rejects_injected_block_policy)
{
  static const uint64_t TEST_TABLE_ID = OB_MIN_USER_OBJECT_ID + 1;
  static const int64_t TEST_SCHEMA_VERSION = 10;
  static const char *BLOCK_POLICY =
      R"({"GRANULARITY":"BLOCK","HOT_RETENTION_INTERVAL":1,"HOT_RETENTION_UNIT":"DAY"})";
  LOG_INFO("========== test_retrive_rejects_injected_block_policy begin ==========",
      K(TEST_TABLE_ID), K(TEST_SCHEMA_VERSION));

  // step 1：构造 full schema，并显式标记为 TIME_POLICY 模拟存量元数据。
  LOG_INFO("step 1: build persisted BLOCK full schema", K(TEST_TABLE_ID));
  share::schema::ObTableSchema block_table_schema;
  block_table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  block_table_schema.set_database_id(OB_SYS_DATABASE_ID);
  block_table_schema.set_table_id(TEST_TABLE_ID);
  block_table_schema.set_table_type(share::schema::USER_TABLE);
  block_table_schema.set_storage_cache_policy_type(ObStorageCachePolicyType::TIME_POLICY);
  OK(block_table_schema.set_table_name("test_retrive_rejects_injected_block_policy"));
  OK(block_table_schema.set_storage_cache_policy(BLOCK_POLICY));
  block_table_schema.set_schema_version(TEST_SCHEMA_VERSION);
  MockBlockPolicySchemaService mock_schema_service(block_table_schema);
  LOG_INFO("step 1 done: BLOCK full schema built",
      K(block_table_schema.get_table_id()), K(block_table_schema.get_storage_cache_policy()),
      "scp_type", block_table_schema.get_storage_cache_policy_type());

  // step 2：构造与 full schema 对应的 simple schema，触发 runtime TIME_POLICY 读取分支。
  LOG_INFO("step 2: build TIME simple schema", K(TEST_TABLE_ID));
  share::schema::ObSimpleTableSchemaV2 simple_table_schema;
  simple_table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  simple_table_schema.set_table_id(TEST_TABLE_ID);
  simple_table_schema.set_table_type(share::schema::USER_TABLE);
  simple_table_schema.set_schema_version(TEST_SCHEMA_VERSION);
  simple_table_schema.set_storage_cache_policy_type(ObStorageCachePolicyType::TIME_POLICY);
  LOG_INFO("step 2 done: TIME simple schema built",
      K(simple_table_schema.get_table_id()),
      "scp_type", simple_table_schema.get_storage_cache_policy_type());
  ASSERT_EQ(ObStorageCachePolicyType::TIME_POLICY,
      simple_table_schema.get_storage_cache_policy_type());

  // step 3：从 mock schema service 加载 BLOCK full schema，验证 runtime guard 直接拒绝。
  LOG_INFO("step 3: retrieve injected BLOCK policy", K(TEST_TABLE_ID));
  ObStorageCachePolicyService policy_service;
  policy_service.tenant_id_ = OB_SYS_TENANT_ID;
  policy_service.schema_service_ = &mock_schema_service;
  ObTablePolicyInfo table_policy_info;
  const int ret = policy_service.retrive_table_policy(&simple_table_schema, table_policy_info);
  LOG_INFO("step 3 done: injected BLOCK policy retrieved",
      KR(ret), K(TEST_TABLE_ID), K(table_policy_info));
  ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  ASSERT_FALSE(table_policy_info.is_inited());
  LOG_INFO("========== test_retrive_rejects_injected_block_policy end ==========",
      K(TEST_TABLE_ID), KR(ret));
}

// ============================================================================
// Follow table 不可服务链路（6 个用例）
// ============================================================================

// ----------------------------------------------------------------------------
// 缺失的直接或逻辑上游会使 follow table 跳过，正常上游与共享辅助表不受影响
//
// 场景：构造普通索引、LOB meta/piece、向量共享表和 per-index 向量辅助表。
// 操作：分别让 data table 或逻辑 domain index 存在、缺失，再检查 follow 链。
// 校验：任一必需上游缺失时 should_skip=true；完整链和非 follow 表返回 false。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_skip_follow_table_with_missing_upstream)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  LOG_INFO("========== test_skip_follow_table_with_missing_upstream begin ==========",
      K(run_ctx_.tenant_id_));

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ASSERT_NE(nullptr, GCTX.schema_service_);

  share::schema::ObSchemaGetterGuard schema_guard;
  OK(GCTX.schema_service_->get_tenant_schema_guard(run_ctx_.tenant_id_, schema_guard));
  const share::schema::ObTableSchema *core_table_schema = nullptr;
  OK(schema_guard.get_table_schema(
      run_ctx_.tenant_id_, OB_ALL_CORE_TABLE_TID, core_table_schema));
  ASSERT_NE(nullptr, core_table_schema);

  ObArray<uint64_t> table_ids;
  OK(schema_guard.get_table_ids_in_tenant(run_ctx_.tenant_id_, table_ids));
  uint64_t max_table_id = 0;
  for (int64_t i = 0; i < table_ids.count(); ++i) {
    if (table_ids.at(i) > max_table_id) {
      max_table_id = table_ids.at(i);
    }
  }
  ASSERT_LT(max_table_id, OB_INVALID_ID - 8);
  const uint64_t missing_table_id = max_table_id + 1;
  bool should_skip = false;

  // step 1：非 follow 表没有上游链，不应被该接口误判为跳过。
  LOG_INFO("step 1: verify non-follow table has no unavailable upstream",
      "table_id", core_table_schema->get_table_id());
  OK(policy_service->check_table_should_skip(
      core_table_schema, should_skip));
  ASSERT_FALSE(should_skip);

  // step 2：普通索引直接跟随 data_table_id，存在时处理、缺失时跳过。
  share::schema::ObSimpleTableSchemaV2 normal_index_schema;
  normal_index_schema.set_tenant_id(run_ctx_.tenant_id_);
  normal_index_schema.set_database_id(core_table_schema->get_database_id());
  normal_index_schema.set_table_id(max_table_id + 2);
  normal_index_schema.set_data_table_id(OB_ALL_CORE_TABLE_TID);
  normal_index_schema.set_table_type(share::schema::USER_INDEX);
  normal_index_schema.set_index_type(share::schema::INDEX_TYPE_NORMAL_LOCAL);
  normal_index_schema.set_index_status(share::schema::INDEX_STATUS_AVAILABLE);
  OK(policy_service->check_table_should_skip(
      &normal_index_schema, should_skip));
  ASSERT_FALSE(should_skip);
  normal_index_schema.set_data_table_id(missing_table_id);
  OK(policy_service->check_table_should_skip(
      &normal_index_schema, should_skip));
  ASSERT_TRUE(should_skip);

  // step 3：LOB meta 与 piece 都直接跟随各自 data_table_id。
  share::schema::ObSimpleTableSchemaV2 orphan_lob_meta_schema;
  orphan_lob_meta_schema.set_tenant_id(run_ctx_.tenant_id_);
  orphan_lob_meta_schema.set_table_id(max_table_id + 3);
  orphan_lob_meta_schema.set_data_table_id(missing_table_id);
  orphan_lob_meta_schema.set_table_type(share::schema::AUX_LOB_META);
  OK(policy_service->check_table_should_skip(
      &orphan_lob_meta_schema, should_skip));
  ASSERT_TRUE(should_skip);

  share::schema::ObSimpleTableSchemaV2 lob_piece_schema;
  lob_piece_schema.set_tenant_id(run_ctx_.tenant_id_);
  lob_piece_schema.set_table_id(max_table_id + 4);
  lob_piece_schema.set_data_table_id(OB_ALL_CORE_TABLE_TID);
  lob_piece_schema.set_table_type(share::schema::AUX_LOB_PIECE);
  OK(policy_service->check_table_should_skip(
      &lob_piece_schema, should_skip));
  ASSERT_FALSE(should_skip);

  // step 4：rowkey_vid/vid_rowkey 是主表级共享表，不绑定某一条 domain index。
  share::schema::ObSimpleTableSchemaV2 vec_shared_schema;
  vec_shared_schema.set_tenant_id(run_ctx_.tenant_id_);
  vec_shared_schema.set_database_id(core_table_schema->get_database_id());
  vec_shared_schema.set_table_id(max_table_id + 5);
  vec_shared_schema.set_data_table_id(OB_ALL_CORE_TABLE_TID);
  vec_shared_schema.set_table_type(share::schema::USER_INDEX);
  vec_shared_schema.set_index_type(share::schema::INDEX_TYPE_VEC_ROWKEY_VID_LOCAL);
  vec_shared_schema.set_index_status(share::schema::INDEX_STATUS_AVAILABLE);
  OK(policy_service->check_table_should_skip(
      &vec_shared_schema, should_skip));
  ASSERT_FALSE(should_skip);

  // SPIV 复用的 rowkey-doc/doc-rowkey 同样是主表级共享表。
  share::schema::ObSimpleTableSchemaV2 spiv_shared_schema;
  spiv_shared_schema.set_tenant_id(run_ctx_.tenant_id_);
  spiv_shared_schema.set_database_id(core_table_schema->get_database_id());
  spiv_shared_schema.set_table_id(max_table_id + 6);
  spiv_shared_schema.set_data_table_id(OB_ALL_CORE_TABLE_TID);
  spiv_shared_schema.set_table_type(share::schema::USER_INDEX);
  spiv_shared_schema.set_index_type(share::schema::INDEX_TYPE_ROWKEY_DOC_ID_LOCAL);
  spiv_shared_schema.set_index_status(share::schema::INDEX_STATUS_AVAILABLE);
  OK(policy_service->check_table_should_skip(
      &spiv_shared_schema, should_skip));
  ASSERT_FALSE(should_skip);

  // step 5：per-index 表的 data_table_id 指向现存主表，但 domain 缺失仍必须跳过。
  share::schema::ObSimpleTableSchemaV2 vec_index_id_schema;
  vec_index_id_schema.set_tenant_id(run_ctx_.tenant_id_);
  vec_index_id_schema.set_database_id(core_table_schema->get_database_id());
  vec_index_id_schema.set_table_id(max_table_id + 7);
  vec_index_id_schema.set_data_table_id(OB_ALL_CORE_TABLE_TID);
  vec_index_id_schema.set_table_type(share::schema::USER_INDEX);
  vec_index_id_schema.set_index_type(share::schema::INDEX_TYPE_VEC_INDEX_ID_LOCAL);
  vec_index_id_schema.set_index_status(share::schema::INDEX_STATUS_AVAILABLE);
  OK(vec_index_id_schema.set_table_name("__idx_1_orphan_vec_index_id"));
  OK(policy_service->check_table_should_skip(
      &vec_index_id_schema, should_skip));
  ASSERT_TRUE(should_skip);

  // 用户主表也缺失时，OB_TABLE_NOT_EXIST 同样归一为跳过。
  vec_index_id_schema.set_data_table_id(missing_table_id);
  OK(policy_service->check_table_should_skip(
      &vec_index_id_schema, should_skip));
  ASSERT_TRUE(should_skip);
  LOG_INFO("========== test_skip_follow_table_with_missing_upstream end ==========",
      K(run_ctx_.tenant_id_), K(missing_table_id));
}

// ----------------------------------------------------------------------------
// 当前表自身或 follow 链任意一级不可服务时，当前对象都必须跳过
//
// 场景：构造 LOB meta -> local index -> user table 的两级 follow 链。
// 操作：依次把 LOB 自身和根表设为 hidden/回收站，再把直接父索引设为不可读。
// 校验：完整链返回 false；当前表或任一级上游不可服务都返回 true；恢复后返回 false。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_skip_table_with_unserviceable_self_or_upstream)
{
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  static constexpr uint64_t ROOT_TABLE_ID = OB_MIN_USER_OBJECT_ID + 4096;
  static constexpr uint64_t PARENT_INDEX_ID = ROOT_TABLE_ID + 1;
  static constexpr uint64_t LOB_META_ID = ROOT_TABLE_ID + 2;
  static constexpr int64_t TEST_SCHEMA_VERSION = 1;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));
  LOG_INFO("========== test_skip_table_with_unserviceable_self_or_upstream begin ==========",
      K(tenant_id), K(ROOT_TABLE_ID), K(PARENT_INDEX_ID), K(LOB_META_ID));

  // step 1：构造完整两级 follow 链并注入独立 schema service，由被测接口按次创建 fresh guard。
  share::schema::ObTableSchema root_table_schema;
  root_table_schema.set_tenant_id(tenant_id);
  root_table_schema.set_table_id(ROOT_TABLE_ID);
  root_table_schema.set_database_id(OB_SYS_DATABASE_ID);
  root_table_schema.set_table_type(share::schema::USER_TABLE);
  root_table_schema.set_schema_version(TEST_SCHEMA_VERSION);
  root_table_schema.set_table_state_flag(share::schema::TABLE_STATE_NORMAL);

  share::schema::ObTableSchema parent_index_schema;
  parent_index_schema.set_tenant_id(tenant_id);
  parent_index_schema.set_table_id(PARENT_INDEX_ID);
  parent_index_schema.set_data_table_id(ROOT_TABLE_ID);
  parent_index_schema.set_database_id(OB_SYS_DATABASE_ID);
  parent_index_schema.set_table_type(share::schema::USER_INDEX);
  parent_index_schema.set_index_type(share::schema::INDEX_TYPE_NORMAL_LOCAL);
  parent_index_schema.set_index_status(share::schema::INDEX_STATUS_AVAILABLE);
  parent_index_schema.set_schema_version(TEST_SCHEMA_VERSION);

  share::schema::ObSimpleTableSchemaV2 lob_meta_schema;
  lob_meta_schema.set_tenant_id(tenant_id);
  lob_meta_schema.set_table_id(LOB_META_ID);
  lob_meta_schema.set_data_table_id(PARENT_INDEX_ID);
  lob_meta_schema.set_database_id(OB_SYS_DATABASE_ID);
  lob_meta_schema.set_table_type(share::schema::AUX_LOB_META);
  lob_meta_schema.set_schema_version(TEST_SCHEMA_VERSION);
  lob_meta_schema.set_table_state_flag(share::schema::TABLE_STATE_NORMAL);

  common::ObSEArray<const share::schema::ObTableSchema *, 2> table_schemas;
  OK(table_schemas.push_back(&root_table_schema));
  OK(table_schemas.push_back(&parent_index_schema));
  MockFollowPolicySchemaService mock_schema_service(table_schemas);

  ObStorageCachePolicyService policy_service;
  policy_service.tenant_id_ = tenant_id;
  policy_service.schema_service_ = &mock_schema_service;
  bool should_skip = true;
  OK(policy_service.check_table_should_skip(
      &lob_meta_schema, should_skip));
  LOG_INFO("step 1 done: complete follow chain remains serviceable", K(should_skip));
  ASSERT_FALSE(should_skip);

  // step 2：当前 LOB 自身 hidden 时应直接跳过，无需查询上游链。
  lob_meta_schema.set_table_state_flag(share::schema::TABLE_STATE_HIDDEN_OFFLINE_DDL);
  OK(policy_service.check_table_should_skip(
      &lob_meta_schema, should_skip));
  LOG_INFO("step 2: hidden current table is skipped", K(should_skip));
  ASSERT_TRUE(should_skip);

  // step 3：根表 hidden 和进入回收站时，递归检查必须穿过正常父索引命中根表。
  lob_meta_schema.set_table_state_flag(share::schema::TABLE_STATE_NORMAL);
  root_table_schema.set_table_state_flag(share::schema::TABLE_STATE_HIDDEN_OFFLINE_DDL);
  OK(policy_service.check_table_should_skip(
      &lob_meta_schema, should_skip));
  LOG_INFO("step 3: hidden root makes leaf unavailable", K(should_skip));
  ASSERT_TRUE(should_skip);

  root_table_schema.set_table_state_flag(share::schema::TABLE_STATE_NORMAL);
  root_table_schema.set_database_id(OB_RECYCLEBIN_SCHEMA_ID);
  OK(policy_service.check_table_should_skip(
      &lob_meta_schema, should_skip));
  LOG_INFO("step 3: recyclebin root makes leaf unavailable", K(should_skip));
  ASSERT_TRUE(should_skip);

  // step 4：直接父索引不可读时无需继续追根，恢复 AVAILABLE 后整条链重新可服务。
  root_table_schema.set_database_id(OB_SYS_DATABASE_ID);
  parent_index_schema.set_index_status(share::schema::INDEX_STATUS_UNAVAILABLE);
  OK(policy_service.check_table_should_skip(
      &lob_meta_schema, should_skip));
  LOG_INFO("step 4: unreadable direct parent makes leaf unavailable", K(should_skip));
  ASSERT_TRUE(should_skip);

  parent_index_schema.set_index_status(share::schema::INDEX_STATUS_AVAILABLE);
  OK(policy_service.check_table_should_skip(
      &lob_meta_schema, should_skip));
  LOG_INFO("step 4 done: recovered follow chain is serviceable", K(should_skip));
  ASSERT_FALSE(should_skip);
  LOG_INFO("========== test_skip_table_with_unserviceable_self_or_upstream end ==========",
      K(tenant_id), K(ROOT_TABLE_ID), K(PARENT_INDEX_ID), K(LOB_META_ID));
}

// ----------------------------------------------------------------------------
// 按 table_id 复查时，当前表或上游不可服务都必须跳过，正常对象不能被误伤
//
// 场景：在独立 mock schema mgr 中构造正常、hidden、回收站、不可读索引及 follow 对象。
// 操作：分别按 table_id 调用 check_table_should_skip_by_table_id_，并检查一个缺失 ID。
// 校验：正常对象返回 false；自身、上游不可服务或当前 schema 缺失时返回 true。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_check_table_should_skip_by_table_id)
{
  static constexpr uint64_t TENANT_ID = OB_SYS_TENANT_ID;
  static constexpr uint64_t NORMAL_TABLE_ID = OB_MIN_USER_OBJECT_ID + 8192;
  static constexpr uint64_t HIDDEN_TABLE_ID = NORMAL_TABLE_ID + 1;
  static constexpr uint64_t RECYCLEBIN_TABLE_ID = NORMAL_TABLE_ID + 2;
  static constexpr uint64_t UNREADABLE_INDEX_ID = NORMAL_TABLE_ID + 3;
  static constexpr uint64_t FOLLOW_NORMAL_ID = NORMAL_TABLE_ID + 4;
  static constexpr uint64_t FOLLOW_HIDDEN_ID = NORMAL_TABLE_ID + 5;
  static constexpr uint64_t MISSING_TABLE_ID = NORMAL_TABLE_ID + 6;
  static constexpr int64_t TEST_SCHEMA_VERSION = 1;
  LOG_INFO("========== test_check_table_should_skip_by_table_id begin ==========",
      K(TENANT_ID), K(NORMAL_TABLE_ID), K(MISSING_TABLE_ID));

  // step 1：把完整 schema 注入独立 mock service，避免改变 mittest 真实租户 schema。
  share::schema::ObTableSchema normal_table_schema;
  normal_table_schema.set_tenant_id(TENANT_ID);
  normal_table_schema.set_table_id(NORMAL_TABLE_ID);
  normal_table_schema.set_database_id(OB_SYS_DATABASE_ID);
  normal_table_schema.set_table_type(share::schema::USER_TABLE);
  normal_table_schema.set_schema_version(TEST_SCHEMA_VERSION);
  normal_table_schema.set_table_state_flag(share::schema::TABLE_STATE_NORMAL);
  OK(normal_table_schema.set_table_name("scp_normal_table"));

  share::schema::ObTableSchema hidden_table_schema;
  hidden_table_schema.set_tenant_id(TENANT_ID);
  hidden_table_schema.set_table_id(HIDDEN_TABLE_ID);
  hidden_table_schema.set_database_id(OB_SYS_DATABASE_ID);
  hidden_table_schema.set_table_type(share::schema::USER_TABLE);
  hidden_table_schema.set_schema_version(TEST_SCHEMA_VERSION);
  hidden_table_schema.set_table_state_flag(share::schema::TABLE_STATE_HIDDEN_OFFLINE_DDL);
  OK(hidden_table_schema.set_table_name("scp_hidden_table"));

  share::schema::ObTableSchema recyclebin_table_schema;
  recyclebin_table_schema.set_tenant_id(TENANT_ID);
  recyclebin_table_schema.set_table_id(RECYCLEBIN_TABLE_ID);
  recyclebin_table_schema.set_database_id(OB_RECYCLEBIN_SCHEMA_ID);
  recyclebin_table_schema.set_table_type(share::schema::USER_TABLE);
  recyclebin_table_schema.set_schema_version(TEST_SCHEMA_VERSION);
  OK(recyclebin_table_schema.set_table_name("scp_recyclebin_table"));

  share::schema::ObTableSchema unreadable_index_schema;
  unreadable_index_schema.set_tenant_id(TENANT_ID);
  unreadable_index_schema.set_table_id(UNREADABLE_INDEX_ID);
  unreadable_index_schema.set_data_table_id(NORMAL_TABLE_ID);
  unreadable_index_schema.set_database_id(OB_SYS_DATABASE_ID);
  unreadable_index_schema.set_table_type(share::schema::USER_INDEX);
  unreadable_index_schema.set_index_type(share::schema::INDEX_TYPE_NORMAL_LOCAL);
  unreadable_index_schema.set_index_status(share::schema::INDEX_STATUS_UNAVAILABLE);
  unreadable_index_schema.set_schema_version(TEST_SCHEMA_VERSION);
  OK(set_mock_table_name(
      unreadable_index_schema, NORMAL_TABLE_ID, "scp_unreadable_index"));

  share::schema::ObTableSchema follow_normal_schema;
  follow_normal_schema.set_tenant_id(TENANT_ID);
  follow_normal_schema.set_table_id(FOLLOW_NORMAL_ID);
  follow_normal_schema.set_data_table_id(NORMAL_TABLE_ID);
  follow_normal_schema.set_database_id(OB_SYS_DATABASE_ID);
  follow_normal_schema.set_table_type(share::schema::AUX_LOB_META);
  follow_normal_schema.set_schema_version(TEST_SCHEMA_VERSION);
  OK(follow_normal_schema.set_table_name("scp_follow_normal_lob_meta"));

  share::schema::ObTableSchema follow_hidden_schema;
  follow_hidden_schema.set_tenant_id(TENANT_ID);
  follow_hidden_schema.set_table_id(FOLLOW_HIDDEN_ID);
  follow_hidden_schema.set_data_table_id(HIDDEN_TABLE_ID);
  follow_hidden_schema.set_database_id(OB_SYS_DATABASE_ID);
  follow_hidden_schema.set_table_type(share::schema::AUX_LOB_META);
  follow_hidden_schema.set_schema_version(TEST_SCHEMA_VERSION);
  OK(follow_hidden_schema.set_table_name("scp_follow_hidden_lob_meta"));

  MockTableIdPolicySchemaService mock_schema_service;
  OK(mock_schema_service.init());
  OK(mock_schema_service.add_table_schema(normal_table_schema, TEST_SCHEMA_VERSION));
  OK(mock_schema_service.add_table_schema(hidden_table_schema, TEST_SCHEMA_VERSION));
  OK(mock_schema_service.add_table_schema(recyclebin_table_schema, TEST_SCHEMA_VERSION));
  OK(mock_schema_service.add_table_schema(unreadable_index_schema, TEST_SCHEMA_VERSION));
  OK(mock_schema_service.add_table_schema(follow_normal_schema, TEST_SCHEMA_VERSION));
  OK(mock_schema_service.add_table_schema(follow_hidden_schema, TEST_SCHEMA_VERSION));

  ObStorageCachePolicyService policy_service;
  policy_service.tenant_id_ = TENANT_ID;
  policy_service.schema_service_ = &mock_schema_service;

  // step 2：正常表和上游正常的 follow 表都应继续参与 SCP。
  bool should_skip = true;
  OK(policy_service.check_table_should_skip_by_table_id_(NORMAL_TABLE_ID, should_skip));
  LOG_INFO("step 2: normal table is not skipped", K(NORMAL_TABLE_ID), K(should_skip));
  ASSERT_FALSE(should_skip);
  OK(policy_service.check_table_should_skip_by_table_id_(FOLLOW_NORMAL_ID, should_skip));
  LOG_INFO("step 2 done: follow table with normal parent is not skipped",
      K(FOLLOW_NORMAL_ID), K(should_skip));
  ASSERT_FALSE(should_skip);

  // step 3：当前表自身 hidden、位于回收站或索引不可读时直接跳过。
  should_skip = false;
  OK(policy_service.check_table_should_skip_by_table_id_(HIDDEN_TABLE_ID, should_skip));
  LOG_INFO("step 3: hidden table is skipped", K(HIDDEN_TABLE_ID), K(should_skip));
  ASSERT_TRUE(should_skip);
  should_skip = false;
  OK(policy_service.check_table_should_skip_by_table_id_(RECYCLEBIN_TABLE_ID, should_skip));
  LOG_INFO("step 3: recyclebin table is skipped", K(RECYCLEBIN_TABLE_ID), K(should_skip));
  ASSERT_TRUE(should_skip);
  should_skip = false;
  OK(policy_service.check_table_should_skip_by_table_id_(UNREADABLE_INDEX_ID, should_skip));
  LOG_INFO("step 3: unreadable index is skipped", K(UNREADABLE_INDEX_ID), K(should_skip));
  ASSERT_TRUE(should_skip);
  LOG_INFO("step 3 done: self-unserviceable tables are skipped",
      K(HIDDEN_TABLE_ID), K(RECYCLEBIN_TABLE_ID), K(UNREADABLE_INDEX_ID));

  // step 4：当前 follow 表正常但 parent hidden，以及当前 schema 已消失，均应跳过。
  should_skip = false;
  OK(policy_service.check_table_should_skip_by_table_id_(FOLLOW_HIDDEN_ID, should_skip));
  LOG_INFO("step 4: follow table with hidden parent is skipped",
      K(FOLLOW_HIDDEN_ID), K(should_skip));
  ASSERT_TRUE(should_skip);
  should_skip = false;
  OK(policy_service.check_table_should_skip_by_table_id_(MISSING_TABLE_ID, should_skip));
  LOG_INFO("step 4: missing table is skipped", K(MISSING_TABLE_ID), K(should_skip));
  ASSERT_TRUE(should_skip);
  LOG_INFO("step 4 done: unavailable upstream and missing current table are skipped",
      K(FOLLOW_HIDDEN_ID), K(MISSING_TABLE_ID));
  LOG_INFO("========== test_check_table_should_skip_by_table_id end ==========",
      K(TENANT_ID), K(NORMAL_TABLE_ID), K(MISSING_TABLE_ID));
}

// ----------------------------------------------------------------------------
// vector per-index aux 必须继承已解析 domain 的 hidden/不可读状态
//
// 场景：构造 user table、vec delta-buffer domain 和按 domain 名生成的 index-id aux。
// 操作：依次把 domain 设置为 hidden、UNAVAILABLE，再恢复为 NORMAL/AVAILABLE。
// 校验：完整链可服务时不跳过；domain 任一不可服务状态都会使 aux 跳过；
//       恢复后重新处理。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_skip_vec_aux_with_unserviceable_domain)
{
  static constexpr uint64_t TENANT_ID = OB_SYS_TENANT_ID;
  static constexpr uint64_t DATA_TABLE_ID = OB_MIN_USER_OBJECT_ID + 9216;
  static constexpr uint64_t DOMAIN_INDEX_ID = DATA_TABLE_ID + 1;
  static constexpr uint64_t AUX_INDEX_ID = DATA_TABLE_ID + 2;
  static constexpr int64_t TEST_SCHEMA_VERSION = 1;
  static constexpr const char *DOMAIN_INDEX_NAME = "scp_vec_domain";
  LOG_INFO("========== test_skip_vec_aux_with_unserviceable_domain begin ==========",
      K(TENANT_ID), K(DATA_TABLE_ID), K(DOMAIN_INDEX_ID), K(AUX_INDEX_ID));

  // step 1：构造按生产命名规则关联的 domain 与 per-index aux，并验证正常链路。
  share::schema::ObTableSchema data_table_schema;
  data_table_schema.set_tenant_id(TENANT_ID);
  data_table_schema.set_table_id(DATA_TABLE_ID);
  data_table_schema.set_database_id(OB_SYS_DATABASE_ID);
  data_table_schema.set_table_type(share::schema::USER_TABLE);
  data_table_schema.set_schema_version(TEST_SCHEMA_VERSION);
  data_table_schema.set_table_state_flag(share::schema::TABLE_STATE_NORMAL);
  OK(data_table_schema.set_table_name("scp_vec_data_table"));

  share::schema::ObTableSchema domain_index_schema;
  domain_index_schema.set_tenant_id(TENANT_ID);
  domain_index_schema.set_table_id(DOMAIN_INDEX_ID);
  domain_index_schema.set_data_table_id(DATA_TABLE_ID);
  domain_index_schema.set_database_id(OB_SYS_DATABASE_ID);
  domain_index_schema.set_table_type(share::schema::USER_INDEX);
  domain_index_schema.set_index_type(share::schema::INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL);
  domain_index_schema.set_index_status(share::schema::INDEX_STATUS_AVAILABLE);
  domain_index_schema.set_schema_version(TEST_SCHEMA_VERSION);
  domain_index_schema.set_table_state_flag(share::schema::TABLE_STATE_NORMAL);
  OK(set_mock_table_name(domain_index_schema, DATA_TABLE_ID, DOMAIN_INDEX_NAME));

  ObSqlString aux_index_name;
  OK(aux_index_name.assign_fmt("%s%s", DOMAIN_INDEX_NAME,
      share::ObVecIndexBuilderUtil::INDEX_ID_TABLE_NAME_SUFFIX));
  share::schema::ObTableSchema aux_index_schema;
  aux_index_schema.set_tenant_id(TENANT_ID);
  aux_index_schema.set_table_id(AUX_INDEX_ID);
  aux_index_schema.set_data_table_id(DATA_TABLE_ID);
  aux_index_schema.set_database_id(OB_SYS_DATABASE_ID);
  aux_index_schema.set_table_type(share::schema::USER_INDEX);
  aux_index_schema.set_index_type(share::schema::INDEX_TYPE_VEC_INDEX_ID_LOCAL);
  aux_index_schema.set_index_status(share::schema::INDEX_STATUS_AVAILABLE);
  aux_index_schema.set_schema_version(TEST_SCHEMA_VERSION);
  aux_index_schema.set_table_state_flag(share::schema::TABLE_STATE_NORMAL);
  OK(set_mock_table_name(aux_index_schema, DATA_TABLE_ID, aux_index_name.ptr()));

  MockTableIdPolicySchemaService mock_schema_service;
  OK(mock_schema_service.init());
  OK(mock_schema_service.add_table_schema(data_table_schema, TEST_SCHEMA_VERSION));
  OK(mock_schema_service.add_table_schema(domain_index_schema, TEST_SCHEMA_VERSION));
  OK(mock_schema_service.add_table_schema(aux_index_schema, TEST_SCHEMA_VERSION));
  ObStorageCachePolicyService policy_service;
  policy_service.tenant_id_ = TENANT_ID;
  policy_service.schema_service_ = &mock_schema_service;
  bool should_skip = true;
  OK(policy_service.check_table_should_skip_by_table_id_(AUX_INDEX_ID, should_skip));
  LOG_INFO("step 1 done: serviceable vec domain keeps aux available",
      K(DOMAIN_INDEX_ID), K(AUX_INDEX_ID), K(should_skip));
  ASSERT_FALSE(should_skip);

  // step 2：按名命中的 domain hidden 时，aux 必须沿逻辑上游链被跳过。
  domain_index_schema.set_table_state_flag(share::schema::TABLE_STATE_HIDDEN_OFFLINE_DDL);
  OK(mock_schema_service.add_table_schema(domain_index_schema, TEST_SCHEMA_VERSION));
  should_skip = false;
  OK(policy_service.check_table_should_skip_by_table_id_(AUX_INDEX_ID, should_skip));
  LOG_INFO("step 2 done: hidden vec domain skips aux",
      K(DOMAIN_INDEX_ID), K(AUX_INDEX_ID), K(should_skip));
  ASSERT_TRUE(should_skip);

  // step 3：domain 恢复 normal 但索引不可读时仍应跳过，完全恢复后重新处理。
  domain_index_schema.set_table_state_flag(share::schema::TABLE_STATE_NORMAL);
  domain_index_schema.set_index_status(share::schema::INDEX_STATUS_UNAVAILABLE);
  OK(mock_schema_service.add_table_schema(domain_index_schema, TEST_SCHEMA_VERSION));
  should_skip = false;
  OK(policy_service.check_table_should_skip_by_table_id_(AUX_INDEX_ID, should_skip));
  LOG_INFO("step 3: unreadable vec domain skips aux",
      K(DOMAIN_INDEX_ID), K(AUX_INDEX_ID), K(should_skip));
  ASSERT_TRUE(should_skip);

  domain_index_schema.set_index_status(share::schema::INDEX_STATUS_AVAILABLE);
  OK(mock_schema_service.add_table_schema(domain_index_schema, TEST_SCHEMA_VERSION));
  should_skip = true;
  OK(policy_service.check_table_should_skip_by_table_id_(AUX_INDEX_ID, should_skip));
  LOG_INFO("step 3 done: recovered vec domain re-enables aux",
      K(DOMAIN_INDEX_ID), K(AUX_INDEX_ID), K(should_skip));
  ASSERT_FALSE(should_skip);
  LOG_INFO("========== test_skip_vec_aux_with_unserviceable_domain end ==========",
      K(TENANT_ID), K(DATA_TABLE_ID), K(DOMAIN_INDEX_ID), K(AUX_INDEX_ID));
}

// ----------------------------------------------------------------------------
// pre-check 与 after-error recheck 连续失败时不能被误判为正常 skip
//
// 场景：在非分区 LOB follow 表扫描中，连续两次 schema guard 获取返回 OB_SCHEMA_EAGAIN。
// 操作：第一次故障命中 pre-check，第二次命中错误后的 serviceability recheck。
// 校验：扫描返回可重试控制流，但首轮保持未完成，证明失败被计数，
//       而不是作为 unavailable skip 吞掉。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_schema_guard_failure_is_not_treated_as_skip)
{
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  static constexpr uint64_t DATA_TABLE_ID = OB_MIN_USER_OBJECT_ID + 10240;
  static constexpr uint64_t LOB_META_ID = DATA_TABLE_ID + 1;
  static constexpr int64_t DATA_TABLET_ID =
      common::ObTabletID::MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID - 5120;
  static constexpr int64_t LOB_TABLET_ID = DATA_TABLET_ID + 1;
  static constexpr int64_t TEST_SCHEMA_VERSION = 1;
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(tenant_id));
  LOG_INFO("========== test_schema_guard_failure_is_not_treated_as_skip begin ==========",
      K(tenant_id), K(DATA_TABLE_ID), K(LOB_META_ID));

  // step 1：构造正常非分区 data/LOB schema，避免 serviceability 本身触发正常 skip。
  MockTableIdPolicySchemaService mock_schema_service;
  OK(mock_schema_service.init());
  OK(add_mock_user_tenant(mock_schema_service, tenant_id, TEST_SCHEMA_VERSION));
  share::schema::ObTableSchema data_table_schema;
  data_table_schema.set_tenant_id(tenant_id);
  data_table_schema.set_table_id(DATA_TABLE_ID);
  data_table_schema.set_database_id(OB_SYS_DATABASE_ID);
  data_table_schema.set_table_type(share::schema::USER_TABLE);
  data_table_schema.set_part_level(share::schema::PARTITION_LEVEL_ZERO);
  data_table_schema.set_tablet_id(common::ObTabletID(DATA_TABLET_ID));
  data_table_schema.set_schema_version(TEST_SCHEMA_VERSION);
  data_table_schema.set_table_state_flag(share::schema::TABLE_STATE_NORMAL);
  data_table_schema.set_storage_cache_policy_type(ObStorageCachePolicyType::HOT_POLICY);
  OK(data_table_schema.set_table_name("scp_guard_failure_data"));

  share::schema::ObTableSchema lob_meta_schema;
  lob_meta_schema.set_tenant_id(tenant_id);
  lob_meta_schema.set_table_id(LOB_META_ID);
  lob_meta_schema.set_data_table_id(DATA_TABLE_ID);
  lob_meta_schema.set_database_id(OB_SYS_DATABASE_ID);
  lob_meta_schema.set_table_type(share::schema::AUX_LOB_META);
  lob_meta_schema.set_part_level(share::schema::PARTITION_LEVEL_ZERO);
  lob_meta_schema.set_tablet_id(common::ObTabletID(LOB_TABLET_ID));
  lob_meta_schema.set_schema_version(TEST_SCHEMA_VERSION);
  lob_meta_schema.set_table_state_flag(share::schema::TABLE_STATE_NORMAL);
  lob_meta_schema.set_storage_cache_policy_type(ObStorageCachePolicyType::MAX_POLICY);
  OK(lob_meta_schema.set_table_name("scp_guard_failure_lob_meta"));
  OK(mock_schema_service.add_table_schema(data_table_schema, TEST_SCHEMA_VERSION));
  OK(mock_schema_service.add_table_schema(lob_meta_schema, TEST_SCHEMA_VERSION));

  // step 2：首个 guard 用于 lite scan；随后连续注入 pre-check 和 recheck 两次失败。
  ObStorageCachePolicyService policy_service;
  OK(policy_service.init(tenant_id));
  policy_service.schema_service_ = &mock_schema_service;
  policy_service.set_need_generate_cache_task(false);
  mock_schema_service.inject_guard_failure(
      2 /*first_failed_guard_call*/, 2 /*failed_guard_call_count*/, OB_SCHEMA_EAGAIN);
  OK(policy_service.check_and_generate_tablet_tasks(
      ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));

  // step 3：两次故障都已命中，且 failed_table_count 语义使首轮保持未完成。
  const int64_t guard_call_count = mock_schema_service.get_guard_call_count();
  LOG_INFO("step 3: verify schema failures remain retryable failures",
      K(guard_call_count), K(policy_service.first_loop_run_competed_), K(DATA_TABLE_ID), K(LOB_META_ID));
  ASSERT_EQ(3, guard_call_count);
  ASSERT_FALSE(policy_service.first_loop_run_competed_);
  LOG_INFO("========== test_schema_guard_failure_is_not_treated_as_skip end ==========",
      K(tenant_id), K(DATA_TABLE_ID), K(LOB_META_ID), K(guard_call_count));
}

// ----------------------------------------------------------------------------
// lite scan 会跳过 hidden parent 的 LOB child、失效旧任务，并在 parent 恢复后重建策略
//
// 场景：partitioned hidden data table 与自身仍为 normal 的 LOB meta 使用不同 part_id，
//       对应正常 LOB 建表时重新分配 partition object id 的行为。
// 操作：为 LOB tablet 预置历史任务和 policy，执行 scan；再恢复 parent 并再次 scan。
// 校验：首轮不处理 LOB partition 且取消旧任务；恢复后重建 table/part/tablet policy。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_lite_scan_skip_and_recover_lob_with_hidden_parent)
{
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  static constexpr uint64_t DATA_TABLE_ID = OB_MIN_USER_OBJECT_ID + 12288;
  static constexpr uint64_t LOB_META_ID = DATA_TABLE_ID + 1;
  static constexpr int64_t DATA_TABLET_ID =
      common::ObTabletID::MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID - 2048;
  static constexpr int64_t LOB_TABLET_ID = DATA_TABLET_ID + 1;
  static constexpr int64_t DATA_PART_ID = 101;
  static constexpr int64_t LOB_PART_ID = 202;
  static constexpr int64_t SCHEMA_VERSION = 10;
  static constexpr int64_t OLD_TABLE_SCHEMA_VERSION = SCHEMA_VERSION - 1;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));
  LOG_INFO("========== test_lite_scan_skip_and_recover_lob_with_hidden_parent begin ==========",
      K(tenant_id), K(DATA_TABLE_ID), K(LOB_META_ID), K(DATA_PART_ID), K(LOB_PART_ID));

  MockTableIdPolicySchemaService mock_schema_service;
  OK(mock_schema_service.init());
  OK(add_mock_user_tenant(mock_schema_service, tenant_id, SCHEMA_VERSION));
  share::schema::ObTableSchema data_table_schema;
  share::schema::ObTableSchema lob_meta_schema;
  OK(init_mock_partitioned_table_schema(
      data_table_schema, tenant_id, DATA_TABLE_ID, OB_INVALID_ID,
      "scp_hidden_data_table", share::schema::USER_TABLE,
      share::schema::INDEX_TYPE_IS_NOT, share::schema::TABLE_STATE_HIDDEN_OFFLINE_DDL,
      DATA_PART_ID, DATA_TABLET_ID, SCHEMA_VERSION,
      ObStorageCachePolicyType::HOT_POLICY));
  OK(init_mock_partitioned_table_schema(
      lob_meta_schema, tenant_id, LOB_META_ID, DATA_TABLE_ID,
      "scp_normal_lob_meta", share::schema::AUX_LOB_META,
      share::schema::INDEX_TYPE_IS_NOT, share::schema::TABLE_STATE_NORMAL,
      LOB_PART_ID, LOB_TABLET_ID, SCHEMA_VERSION,
      ObStorageCachePolicyType::MAX_POLICY));
  OK(mock_schema_service.add_table_schema(data_table_schema, SCHEMA_VERSION));
  OK(mock_schema_service.add_table_schema(lob_meta_schema, SCHEMA_VERSION));

  ObStorageCachePolicyService policy_service;
  OK(policy_service.init(tenant_id));
  policy_service.schema_service_ = &mock_schema_service;
  policy_service.set_need_generate_cache_task(false);
  ObStorageCacheTabletTask *old_task = OB_NEW(
      ObStorageCacheTabletTask, ObMemAttr(tenant_id, "SCPFollowScan"));
  ASSERT_NE(nullptr, old_task);
  ObStorageCacheTabletTaskHandle old_task_handle;
  old_task_handle.set_ptr(old_task);
  OK(old_task->init_partition_task(tenant_id, LOB_TABLET_ID, PolicyStatus::HOT));
  OK(old_task->set_status(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED));
  OK(policy_service.tablet_scheduler_.tablet_task_map_.set_refactored(
      LOB_TABLET_ID, old_task_handle));
  OK(policy_service.tablet_status_map_.set_refactored(LOB_TABLET_ID, PolicyStatus::HOT));

  ObStorageCachePolicy old_table_policy;
  OK(old_table_policy.set_global_policy(ObStorageCacheGlobalPolicy::HOT_POLICY));
  ObTablePolicyInfo old_table_policy_info;
  OK(old_table_policy_info.init(
      LOB_META_ID, old_table_policy, OLD_TABLE_SCHEMA_VERSION));
  OK(policy_service.table_policy_map_.set_refactored(
      LOB_META_ID, old_table_policy_info));
  share::schema::ObPartition old_lob_partition;
  old_lob_partition.set_tenant_id(tenant_id);
  old_lob_partition.set_table_id(LOB_META_ID);
  old_lob_partition.set_part_id(LOB_PART_ID);
  old_lob_partition.set_tablet_id(common::ObTabletID(LOB_TABLET_ID));
  old_lob_partition.set_schema_version(SCHEMA_VERSION);
  old_lob_partition.set_part_storage_cache_policy_type(ObStorageCachePolicyType::NONE_POLICY);
  ObTabletPolicyInfo old_lob_part_policy_info;
  OK(old_lob_part_policy_info.init(&old_lob_partition));
  OK(old_lob_part_policy_info.set_tablet_policy_status(PolicyStatus::COLD));
  OK(policy_service.part_policy_map_.set_refactored(
      LOB_PART_ID, old_lob_part_policy_info));

  // 首轮必须跳过 LOB partition；part map 保持 COLD 可证明它没有被错误重算。
  OK(policy_service.check_and_generate_tablet_tasks(
      ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  LOG_INFO("hidden upstream scan finished", K(policy_service.first_loop_run_competed_));
  ASSERT_TRUE(policy_service.first_loop_run_competed_);
  ObStorageCacheTabletTaskHandle stored_task_handle;
  OK(policy_service.tablet_scheduler_.tablet_task_map_.get_refactored(
      LOB_TABLET_ID, stored_task_handle));
  ASSERT_TRUE(stored_task_handle.is_valid());
  ASSERT_EQ(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED,
      stored_task_handle()->get_status());
  PolicyStatus tablet_status = PolicyStatus::MAX_STATUS;
  ASSERT_EQ(OB_HASH_NOT_EXIST,
      policy_service.tablet_status_map_.get_refactored(LOB_TABLET_ID, tablet_status));
  ObTablePolicyInfo table_policy_info;
  ASSERT_EQ(OB_HASH_NOT_EXIST,
      policy_service.table_policy_map_.get_refactored(LOB_META_ID, table_policy_info));
  ObTabletPolicyInfo part_policy_info;
  OK(policy_service.part_policy_map_.get_refactored(LOB_PART_ID, part_policy_info));
  ASSERT_EQ(PolicyStatus::COLD, part_policy_info.get_tablet_policy_status());

  // parent 恢复后，data 与 LOB 使用各自 part_id 重建，不依赖跨表 key 冲突。
  data_table_schema.set_table_state_flag(share::schema::TABLE_STATE_NORMAL);
  OK(mock_schema_service.add_table_schema(data_table_schema, SCHEMA_VERSION));
  policy_service.first_loop_run_competed_ = false;
  OK(policy_service.check_and_generate_tablet_tasks(
      ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  LOG_INFO("recovered hidden upstream scan finished",
      K(policy_service.first_loop_run_competed_), K(DATA_PART_ID), K(LOB_PART_ID));
  ASSERT_TRUE(policy_service.first_loop_run_competed_);
  OK(policy_service.table_policy_map_.get_refactored(LOB_META_ID, table_policy_info));
  OK(policy_service.part_policy_map_.get_refactored(LOB_PART_ID, part_policy_info));
  ASSERT_EQ(SCHEMA_VERSION, part_policy_info.get_tablet_schema_version());
  ASSERT_EQ(PolicyStatus::HOT, part_policy_info.get_tablet_policy_status());
  OK(policy_service.tablet_status_map_.get_refactored(LOB_TABLET_ID, tablet_status));
  ASSERT_EQ(PolicyStatus::HOT, tablet_status);
  LOG_INFO("========== test_lite_scan_skip_and_recover_lob_with_hidden_parent end ==========",
      K(tenant_id), K(DATA_TABLE_ID), K(LOB_META_ID), K(LOB_TABLET_ID));
}

// ============================================================================
// Partition policy ID 复用（2 个用例）
// ============================================================================

// ----------------------------------------------------------------------------
// remap 后的新索引会覆盖共享 part_id 上旧 hidden 索引留下的 policy
//
// 场景：old hidden index 与 new normal index 使用不同 table_id，但复用同一个 part_id；
//       old index 留下的 policy schema version 大于 new index 的 partition schema version。
// 操作：预置较新版本的 COLD part policy 后执行完整 lite scan。
// 校验：强制重算不读取旧版本做单调性校验，new index 以较小版本覆盖旧 policy 并继承 HOT。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_lite_scan_recalculate_remapped_index_partition)
{
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  static constexpr uint64_t DATA_TABLE_ID = OB_MIN_USER_OBJECT_ID + 13312;
  static constexpr uint64_t OLD_INDEX_ID = DATA_TABLE_ID + 1;
  static constexpr uint64_t NEW_INDEX_ID = DATA_TABLE_ID + 2;
  static constexpr int64_t DATA_TABLET_ID =
      common::ObTabletID::MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID - 3072;
  static constexpr int64_t INDEX_TABLET_ID = DATA_TABLET_ID + 1;
  static constexpr int64_t DATA_PART_ID = 301;
  static constexpr int64_t REMAPPED_PART_ID = 401;
  static constexpr int64_t PART_SCHEMA_VERSION = 20;
  static constexpr int64_t STALE_PART_SCHEMA_VERSION = PART_SCHEMA_VERSION + 1;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));
  LOG_INFO("========== test_lite_scan_recalculate_remapped_index_partition begin ==========",
      K(tenant_id), K(OLD_INDEX_ID), K(NEW_INDEX_ID), K(REMAPPED_PART_ID));

  MockTableIdPolicySchemaService mock_schema_service;
  OK(mock_schema_service.init());
  OK(add_mock_user_tenant(mock_schema_service, tenant_id, PART_SCHEMA_VERSION));
  share::schema::ObTableSchema data_table_schema;
  share::schema::ObTableSchema old_index_schema;
  share::schema::ObTableSchema new_index_schema;
  OK(init_mock_partitioned_table_schema(
      data_table_schema, tenant_id, DATA_TABLE_ID, OB_INVALID_ID,
      "scp_remap_data_table", share::schema::USER_TABLE,
      share::schema::INDEX_TYPE_IS_NOT, share::schema::TABLE_STATE_NORMAL,
      DATA_PART_ID, DATA_TABLET_ID, PART_SCHEMA_VERSION,
      ObStorageCachePolicyType::HOT_POLICY));
  OK(init_mock_partitioned_table_schema(
      old_index_schema, tenant_id, OLD_INDEX_ID, DATA_TABLE_ID,
      "scp_old_hidden_index", share::schema::USER_INDEX,
      share::schema::INDEX_TYPE_NORMAL_LOCAL, share::schema::TABLE_STATE_HIDDEN_OFFLINE_DDL,
      REMAPPED_PART_ID, INDEX_TABLET_ID, PART_SCHEMA_VERSION,
      ObStorageCachePolicyType::NONE_POLICY));
  OK(init_mock_partitioned_table_schema(
      new_index_schema, tenant_id, NEW_INDEX_ID, DATA_TABLE_ID,
      "scp_new_normal_index", share::schema::USER_INDEX,
      share::schema::INDEX_TYPE_NORMAL_LOCAL, share::schema::TABLE_STATE_NORMAL,
      REMAPPED_PART_ID, INDEX_TABLET_ID, PART_SCHEMA_VERSION,
      ObStorageCachePolicyType::NONE_POLICY));
  OK(mock_schema_service.add_table_schema(data_table_schema, PART_SCHEMA_VERSION));
  OK(mock_schema_service.add_table_schema(old_index_schema, PART_SCHEMA_VERSION));
  OK(mock_schema_service.add_table_schema(new_index_schema, PART_SCHEMA_VERSION));

  ObStorageCachePolicyService policy_service;
  OK(policy_service.init(tenant_id));
  policy_service.schema_service_ = &mock_schema_service;
  policy_service.set_need_generate_cache_task(false);
  share::schema::ObPartition old_index_partition;
  old_index_partition.set_tenant_id(tenant_id);
  old_index_partition.set_table_id(OLD_INDEX_ID);
  old_index_partition.set_part_id(REMAPPED_PART_ID);
  old_index_partition.set_tablet_id(common::ObTabletID(INDEX_TABLET_ID));
  old_index_partition.set_schema_version(STALE_PART_SCHEMA_VERSION);
  old_index_partition.set_part_storage_cache_policy_type(ObStorageCachePolicyType::NONE_POLICY);
  ObTabletPolicyInfo old_index_part_policy_info;
  OK(old_index_part_policy_info.init(&old_index_partition));
  OK(old_index_part_policy_info.set_tablet_policy_status(PolicyStatus::COLD));
  OK(policy_service.part_policy_map_.set_refactored(
      REMAPPED_PART_ID, old_index_part_policy_info));

  OK(policy_service.check_and_generate_tablet_tasks(
      ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  LOG_INFO("remapped index scan finished",
      K(policy_service.first_loop_run_competed_), K(REMAPPED_PART_ID));
  ASSERT_TRUE(policy_service.first_loop_run_competed_);
  ObTabletPolicyInfo remapped_part_policy_info;
  OK(policy_service.part_policy_map_.get_refactored(
      REMAPPED_PART_ID, remapped_part_policy_info));
  LOG_INFO("verify remapped index overwrites newer stale part policy",
      K(STALE_PART_SCHEMA_VERSION), K(PART_SCHEMA_VERSION),
      "actual_schema_version", remapped_part_policy_info.get_tablet_schema_version(),
      "actual_policy_status", remapped_part_policy_info.get_tablet_policy_status());
  ASSERT_EQ(PART_SCHEMA_VERSION,
      remapped_part_policy_info.get_tablet_schema_version());
  ASSERT_EQ(PolicyStatus::HOT,
      remapped_part_policy_info.get_tablet_policy_status());
  PolicyStatus tablet_status = PolicyStatus::MAX_STATUS;
  OK(policy_service.tablet_status_map_.get_refactored(INDEX_TABLET_ID, tablet_status));
  ASSERT_EQ(PolicyStatus::HOT, tablet_status);
  LOG_INFO("========== test_lite_scan_recalculate_remapped_index_partition end ==========",
      K(tenant_id), K(OLD_INDEX_ID), K(NEW_INDEX_ID), K(REMAPPED_PART_ID));
}

// ----------------------------------------------------------------------------
// 强制重算 subpartition 时允许新 schema version 小于同 ID 的历史 policy version
//
// 场景：历史对象与新对象复用 sub_part_id，历史 policy 的 schema version 更大。
// 操作：以 sub_partition_policy_need_calc=true 直接处理新对象。
// 校验：不读取历史版本做单调性校验，并以新版本和重新计算的有效状态覆盖旧 policy。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_recalculate_reused_subpartition_with_older_version)
{
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  static constexpr uint64_t TABLE_ID = OB_MIN_USER_OBJECT_ID + 14336;
  static constexpr int64_t TABLET_ID =
      common::ObTabletID::MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID - 4096;
  static constexpr int64_t PART_ID = 501;
  static constexpr int64_t SUBPART_ID = 502;
  static constexpr int64_t NEW_SCHEMA_VERSION = 30;
  static constexpr int64_t STALE_SCHEMA_VERSION = NEW_SCHEMA_VERSION + 1;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));
  LOG_INFO("========== test_recalculate_reused_subpartition_with_older_version begin ==========",
      K(tenant_id), K(TABLE_ID), K(SUBPART_ID), K(NEW_SCHEMA_VERSION), K(STALE_SCHEMA_VERSION));

  // step 1：预置同 sub_part_id 的较新历史 policy，模拟旧对象残留。
  ObStorageCachePolicyService policy_service;
  OK(policy_service.init(tenant_id));
  policy_service.set_need_generate_cache_task(false);
  share::schema::ObSubPartition stale_subpartition;
  stale_subpartition.set_tenant_id(tenant_id);
  stale_subpartition.set_table_id(TABLE_ID);
  stale_subpartition.set_part_id(PART_ID);
  stale_subpartition.set_sub_part_id(SUBPART_ID);
  stale_subpartition.set_tablet_id(common::ObTabletID(TABLET_ID));
  stale_subpartition.set_schema_version(STALE_SCHEMA_VERSION);
  stale_subpartition.set_part_storage_cache_policy_type(ObStorageCachePolicyType::COLD_POLICY);
  ObTabletPolicyInfo stale_policy_info;
  OK(stale_policy_info.init(&stale_subpartition));
  OK(stale_policy_info.set_tablet_policy_status(PolicyStatus::COLD));
  OK(policy_service.part_policy_map_.set_refactored(SUBPART_ID, stale_policy_info));
  LOG_INFO("step 1 done: stale subpartition policy inserted",
      K(SUBPART_ID), K(STALE_SCHEMA_VERSION), K(stale_policy_info));

  // step 2：强制重算复用 ID 的较旧新对象，必须直接覆盖而不是报版本倒退。
  share::schema::ObSubPartition new_subpartition;
  new_subpartition.set_tenant_id(tenant_id);
  new_subpartition.set_table_id(TABLE_ID);
  new_subpartition.set_part_id(PART_ID);
  new_subpartition.set_sub_part_id(SUBPART_ID);
  new_subpartition.set_tablet_id(common::ObTabletID(TABLET_ID));
  new_subpartition.set_schema_version(NEW_SCHEMA_VERSION);
  new_subpartition.set_part_storage_cache_policy_type(ObStorageCachePolicyType::HOT_POLICY);
  ObStorageCachePolicy table_policy;
  OK(table_policy.set_global_policy(ObStorageCacheGlobalPolicy::HOT_POLICY));
  OK(policy_service.handle_subpartitions(
      &new_subpartition,
      table_policy,
      PolicyStatus::HOT,
      ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL,
      true /*sub_partition_policy_need_calc*/));

  // step 3：验证历史较大版本已被新对象的较小版本和有效状态覆盖。
  ObTabletPolicyInfo new_policy_info;
  OK(policy_service.part_policy_map_.get_refactored(SUBPART_ID, new_policy_info));
  LOG_INFO("step 3: verify recalculated subpartition policy",
      K(SUBPART_ID), K(NEW_SCHEMA_VERSION), K(STALE_SCHEMA_VERSION), K(new_policy_info));
  ASSERT_EQ(NEW_SCHEMA_VERSION, new_policy_info.get_tablet_schema_version());
  ASSERT_NE(PolicyStatus::MAX_STATUS, new_policy_info.get_tablet_policy_status());
  LOG_INFO("========== test_recalculate_reused_subpartition_with_older_version end ==========",
      K(tenant_id), K(TABLE_ID), K(SUBPART_ID));
}

// ============================================================================
// Restart retry scheduling（1 个用例）
// ============================================================================

// ----------------------------------------------------------------------------
// 首轮未完成时，快速重试窗口结束后仍按一小时间隔持续重试
//
// 场景：首轮始终未完成，并依次模拟启动后一小时内、一小时边界和慢速重试间隔。
// 操作：直接推进逻辑时间并记录最近一次慢速重试时间。
// 校验：首小时始终重试；超时后立即重试一次；之后每满一小时重试；完成后停止。
// ----------------------------------------------------------------------------
TEST_F(ObStorageCachePolicyColdTest, test_restart_retry_continues_hourly_after_timeout)
{
  static constexpr int64_t FIRST_RESTART_TIME_S = 100;
  static constexpr int64_t RETRY_INTERVAL_S =
      ObStorageCachePolicyService::RESTART_RETRY_TIME_THRESHOLD_S;
  ObStorageCachePolicyService policy_service;
  policy_service.first_loop_run_competed_ = false;
  policy_service.first_restart_time_s_ = FIRST_RESTART_TIME_S;
  policy_service.last_restart_retry_time_s_ = 0;
  LOG_INFO("========== test_restart_retry_continues_hourly_after_timeout begin ==========",
      K(FIRST_RESTART_TIME_S), K(RETRY_INTERVAL_S));

  // step 1：首小时沿用定时任务的快速重试节奏，边界到达时立即进入首次慢速重试。
  ASSERT_TRUE(policy_service.need_restart_retry_(FIRST_RESTART_TIME_S));
  ASSERT_TRUE(policy_service.need_restart_retry_(
      FIRST_RESTART_TIME_S + RETRY_INTERVAL_S - 1));
  ASSERT_TRUE(policy_service.need_restart_retry_(
      FIRST_RESTART_TIME_S + RETRY_INTERVAL_S));
  LOG_INFO("step 1 done: retry remains due through the fast-window boundary",
      K(policy_service.first_restart_time_s_), K(policy_service.last_restart_retry_time_s_));

  // step 2：记录首次慢速重试后，一小时内不重复扫描，满一小时再次触发。
  policy_service.last_restart_retry_time_s_ = FIRST_RESTART_TIME_S + RETRY_INTERVAL_S;
  ASSERT_FALSE(policy_service.need_restart_retry_(
      policy_service.last_restart_retry_time_s_ + RETRY_INTERVAL_S - 1));
  ASSERT_TRUE(policy_service.need_restart_retry_(
      policy_service.last_restart_retry_time_s_ + RETRY_INTERVAL_S));
  LOG_INFO("step 2 done: slow retry is limited to once per hour",
      K(policy_service.last_restart_retry_time_s_), K(RETRY_INTERVAL_S));

  // step 3：首轮完成后，无论时间是否满足都不得继续重试。
  policy_service.first_loop_run_competed_ = true;
  ASSERT_FALSE(policy_service.need_restart_retry_(
      policy_service.last_restart_retry_time_s_ + 2 * RETRY_INTERVAL_S));
  LOG_INFO("========== test_restart_retry_continues_hourly_after_timeout end ==========",
      K(policy_service.first_loop_run_competed_), K(policy_service.last_restart_retry_time_s_));
}

}
}

int main(int argc, char **argv)
{
  char buf[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("20G");
  GCONF.memory_limit.set_value("10G");
  GCONF.system_memory.set_value("3G");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
