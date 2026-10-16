/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gmock/gmock.h>
#define private public
#define protected public
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_cluster_version.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_iterator.h"
#include "src/storage/ss_garbage_collector/dag/ob_ss_gc_exec_dag.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#undef protected
#undef private
#include "lib/alloc/ob_malloc_callback.h"
#include "observer/mysql/obmp_base.h"
#include "share/ob_upgrade_utils.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_util.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
char *shared_storage_info = nullptr;
using namespace unittest;
namespace storage
{
using namespace common;

class ObServerModeGuard
{
public:
  explicit ObServerModeGuard(const observer::ObServerMode mode)
    : old_mode_(GCTX.startup_mode_)
  {
    GCTX.startup_mode_ = mode;
  }
  ~ObServerModeGuard()
  {
    GCTX.startup_mode_ = old_mode_;
  }

private:
  observer::ObServerMode old_mode_;
};

// Each test owns a unique round_id band to avoid cross-test pollution; sub-tests
// pick task_ids inside the band. Tenant_id is sys (1) so the operator's
// get_real_tenant_id_ leaves it as-is and the rows land in sys tenant's copy of
// __all_shared_gc_task / __all_shared_gc_task_history.
class TestSharedGCTaskTableOperator : public ObSimpleClusterTestBase
{
public:
  TestSharedGCTaskTableOperator()
    : ObSimpleClusterTestBase("test_shared_gc_task_table_operator") {}

  static const uint64_t TENANT_ID = OB_SYS_TENANT_ID;

  // Build a Fail-GC value for tests (deterministic content for round-trip checks)
  static int build_fail_value(ObIAllocator &allocator,
                              ObSharedGCFailInfo &info,
                              ObString &value);
  // Build a MarkSweep-GC value (id_count macro ids, deterministic seeds)
  static int build_mark_sweep_value(ObIAllocator &allocator,
                                    int64_t id_count,
                                    ObSharedGCMacroIdList &list,
                                    ObString &value);
  // 构造按存储位置分类的 MarkSweepGC 宏块列表。
  static int build_mark_sweep_stat_value(ObIAllocator &allocator,
                                         const int64_t in_table_count,
                                         const int64_t object_storage_count,
                                         ObSharedGCMacroIdList &list,
                                         ObString &value);
  // Build a task with a given key + Fail value (tablet meta gc info).
  static int build_fail_task(ObIAllocator &allocator,
                             const int64_t round_id,
                             const int64_t task_id,
                             ObSharedGCTask &task,
                             ObSharedGCFailInfo &info_for_check);

  static int build_history_tasks(
      ObIAllocator &allocator,
      const int64_t round_id,
      const int64_t task_count,
      const int64_t value_size,
      ObSharedGCTaskIArray &tasks);
  static int get_history_count(
      const int64_t round_id,
      ObISQLClient &client,
      int64_t &row_count,
      int64_t &value_length);

  int insert_histories_with_memory(
      const ObSharedGCTaskIArray &tasks,
      int64_t &peak_memory,
      int64_t &retained_memory,
      int64_t &sql_peak_memory,
      int64_t &sql_retained_memory,
      int64_t &write_count);

  ObISQLClient &sql_proxy() { return get_curr_simple_server().get_observer().get_mysql_proxy(); }

  // Insert a history row then backdate its gmt_create to make it appear age_us microseconds old.
  static int insert_history_backdated(const uint64_t tenant_id,
                                      const int64_t round_id,
                                      const int64_t task_id,
                                      const int64_t age_us,
                                      ObISQLClient &client);
  static int backdate_active_task_modified(const ObSharedGCTaskKey &key,
                                           const int64_t age_us,
                                           ObISQLClient &client);
  static int get_active_task_modified_us(const ObSharedGCTaskKey &key,
                                         ObISQLClient &client,
                                         int64_t &modified_us);
  // Delete all history rows with the given round_id; used as test cleanup.
  static int delete_history_by_round(const uint64_t tenant_id,
                                     const int64_t round_id,
                                     ObISQLClient &client);
};

const uint64_t TestSharedGCTaskTableOperator::TENANT_ID;

int TestSharedGCTaskTableOperator::build_fail_value(
    ObIAllocator &allocator,
    ObSharedGCFailInfo &info,
    ObString &value)
{
  info.reset();
  info.meta_type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  info.op_id_ = 12345;
  info.ls_id_ = share::ObLSID(1001);
  info.tablet_id_ = ObTabletID(2001);
  info.transfer_scn_.set_min();
  return info.to_value(allocator, value);
}

int TestSharedGCTaskTableOperator::build_mark_sweep_value(
    ObIAllocator &allocator,
    int64_t id_count,
    ObSharedGCMacroIdList &list,
    ObString &value)
{
  int ret = OB_SUCCESS;
  list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < id_count; ++i) {
    blocksstable::MacroBlockId id(
        blocksstable::MacroBlockId::DEFAULT_FIRST_ID, static_cast<int64_t>(10000 + i), 0, 0, 0);
    if (OB_FAIL(list.add(id))) {
      LOG_WARN("add failed", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    ret = list.to_value(allocator, value);
  }
  return ret;
}

int TestSharedGCTaskTableOperator::build_mark_sweep_stat_value(
    ObIAllocator &allocator,
    const int64_t in_table_count,
    const int64_t object_storage_count,
    ObSharedGCMacroIdList &list,
    ObString &value)
{
  int ret = OB_SUCCESS;
  const int64_t total_count = in_table_count + object_storage_count;
  blocksstable::MacroBlockId macro_id;
  list.reset();
  if (OB_UNLIKELY(in_table_count < 0 || object_storage_count < 0 || total_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < total_count; ++i) {
    const bool is_in_table = i < in_table_count;
    macro_id.reset();
    macro_id.set_id_mode(static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(is_in_table
        ? blocksstable::ObStorageObjectType::SHARED_TABLET_SUB_META
        : blocksstable::ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
    macro_id.set_location(is_in_table
        ? blocksstable::ObMacroBlockIdLocation::TABLE
        : blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
    macro_id.set_second_id(5000 + i);
    macro_id.set_reorganization_scn(100);
    macro_id.set_op_id(200);
    macro_id.set_seq_id(300 + i);
    if (OB_UNLIKELY(!macro_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(list.add(macro_id))) {
      LOG_WARN("fail to add macro id", KR(ret), K(macro_id));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(list.to_value(allocator, value))) {
    LOG_WARN("fail to serialize macro id list", KR(ret), K(total_count));
  }
  return ret;
}

int TestSharedGCTaskTableOperator::build_fail_task(
    ObIAllocator &allocator,
    const int64_t round_id,
    const int64_t task_id,
    ObSharedGCTask &task,
    ObSharedGCFailInfo &info_for_check)
{
  int ret = OB_SUCCESS;
  task.reset();
  task.key_ = ObSharedGCTaskKey(TENANT_ID, ObSharedGCTaskType::FAIL_GC,
                                round_id, task_id);
  ObString fail_value;
  if (OB_FAIL(build_fail_value(allocator, info_for_check, fail_value))) {
    LOG_WARN("build_fail_value failed", K(ret));
  } else {
    task.value_ = fail_value;
    task.extra_info_ = ObString::make_string("initial diagnostic");
    task.server_id_ = ObSharedGCTask::INVALID_SERVER_ID;
    task.trace_id_.reset();
  }
  return ret;
}

// =====================================================================
// DELETE_TENANT / DELETE_SERVER 定义相关测试
// =====================================================================

TEST(SharedGCTaskDefine, delete_task_types_and_values)
{
  const uint64_t user_tenant_id = 1002;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  const uint64_t server_id = 900001;
  const int64_t delete_ts = 123456789;
  ObArenaAllocator allocator("DelGCDefine");

  ASSERT_TRUE(is_valid_shared_gc_task_type(ObSharedGCTaskType::DELETE_TENANT));
  ASSERT_TRUE(is_valid_shared_gc_task_type(ObSharedGCTaskType::DELETE_SERVER));
  ASSERT_TRUE(is_delete_tenant_type(ObSharedGCTaskType::DELETE_TENANT));
  ASSERT_TRUE(is_delete_server_type(ObSharedGCTaskType::DELETE_SERVER));
  ASSERT_FALSE(is_shared_data_gc_task_type(ObSharedGCTaskType::LS_META_GC));
  ASSERT_FALSE(is_shared_data_gc_task_type(ObSharedGCTaskType::DELETE_TENANT));
  ASSERT_FALSE(is_shared_data_gc_task_type(ObSharedGCTaskType::DELETE_SERVER));
  ASSERT_STREQ("DELETE_TENANT",
      ob_shared_gc_task_type_to_string(ObSharedGCTaskType::DELETE_TENANT));
  ASSERT_STREQ("DELETE_SERVER",
      ob_shared_gc_task_type_to_string(ObSharedGCTaskType::DELETE_SERVER));
  ASSERT_EQ(ObSharedGCTaskType::DELETE_TENANT,
      ob_string_to_shared_gc_task_type(ObString::make_string("DELETE_TENANT")));
  ASSERT_EQ(ObSharedGCTaskType::DELETE_SERVER,
      ob_string_to_shared_gc_task_type(ObString::make_string("DELETE_SERVER")));

  ObDeleteTenantTaskValue tenant_value;
  tenant_value.tenant_id_ = user_tenant_id;
  tenant_value.delete_ts_ = delete_ts;
  ASSERT_TRUE(tenant_value.is_valid());
  ObString tenant_buf;
  ASSERT_EQ(OB_SUCCESS, tenant_value.to_value(allocator, tenant_buf));
  ObDeleteTenantTaskValue decoded_tenant_value;
  ASSERT_EQ(OB_SUCCESS, decoded_tenant_value.from_value(tenant_buf));
  ASSERT_EQ(user_tenant_id, decoded_tenant_value.tenant_id_);
  ASSERT_EQ(delete_ts, decoded_tenant_value.delete_ts_);

  tenant_value.tenant_id_ = meta_tenant_id;
  ASSERT_TRUE(tenant_value.is_valid());
  tenant_value.tenant_id_ = OB_SYS_TENANT_ID;
  ASSERT_FALSE(tenant_value.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, tenant_value.to_value(allocator, tenant_buf));
  tenant_value.tenant_id_ = user_tenant_id;
  tenant_value.delete_ts_ = 0;
  ASSERT_FALSE(tenant_value.is_valid());

  ObAddr server_addr;
  ASSERT_TRUE(server_addr.set_ip_addr("127.0.0.88", 2882));
  ObDeleteServerTaskValue server_value;
  server_value.server_id_ = server_id;
  server_value.server_addr_ = server_addr;
  server_value.delete_ts_ = delete_ts;
  ASSERT_TRUE(server_value.is_valid());
  ObString server_buf;
  ASSERT_EQ(OB_SUCCESS, server_value.to_value(allocator, server_buf));
  ObDeleteServerTaskValue decoded_server_value;
  ASSERT_EQ(OB_SUCCESS, decoded_server_value.from_value(server_buf));
  ASSERT_EQ(server_id, decoded_server_value.server_id_);
  ASSERT_TRUE(server_addr == decoded_server_value.server_addr_);
  ASSERT_EQ(delete_ts, decoded_server_value.delete_ts_);

  server_value.server_id_ = 0;
  ASSERT_FALSE(server_value.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, server_value.to_value(allocator, server_buf));
  server_value.server_id_ = server_id;
  server_value.server_addr_.reset();
  ASSERT_FALSE(server_value.is_valid());
  server_value.server_addr_ = server_addr;
  server_value.delete_ts_ = 0;
  ASSERT_FALSE(server_value.is_valid());
}

TEST_F(TestSharedGCTaskTableOperator, ls_ckpt_gc_task_round_trip)
{
  share::SCN meta_scn;
  ASSERT_EQ(OB_SUCCESS, meta_scn.convert_for_inner_table_field(
      static_cast<uint64_t>(ObTimeUtility::current_time_ns())));
  ASSERT_TRUE(is_valid_shared_gc_task_type(ObSharedGCTaskType::LS_CKPT_GC));
  ASSERT_TRUE(is_ls_ckpt_gc_type(ObSharedGCTaskType::LS_CKPT_GC));
  ASSERT_FALSE(is_shared_data_gc_task_type(ObSharedGCTaskType::LS_CKPT_GC));
  ASSERT_STREQ("LS_CKPT_GC",
      ob_shared_gc_task_type_to_string(ObSharedGCTaskType::LS_CKPT_GC));
  ASSERT_EQ(ObSharedGCTaskType::LS_CKPT_GC,
      ob_string_to_shared_gc_task_type(ObString::make_string("LS_CKPT_GC")));

  ObArenaAllocator value_allocator("CkptGCValue");
  ObSharedGCLSCheckpointInfo value;
  value.ls_id_ = share::SYS_LS;
  value.meta_scn_ = meta_scn;
  ObString serialized_value;
  ASSERT_EQ(OB_SUCCESS, value.to_value(value_allocator, serialized_value));
  ObSharedGCLSCheckpointInfo decoded_value;
  ASSERT_EQ(OB_SUCCESS, decoded_value.from_value(serialized_value));
  ASSERT_EQ(value.ls_id_, decoded_value.ls_id_);
  ASSERT_EQ(value.meta_scn_, decoded_value.meta_scn_);

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskUtil::generate_ls_ckpt_gc_task(
      TENANT_ID, value.ls_id_, value.meta_scn_, sql_proxy()));
  ObSharedGCTask inserted_task;
  bool task_found = false;
  int ret = OB_SUCCESS;
  ObArenaAllocator task_allocator("CkptGCTaskIt");
  ObSharedGCTaskTypeFilter type_filter;
  ObSharedGCTaskRoundIdFilter round_id_filter;
  ObSharedGCTaskTableFullIterator task_iter;
  int64_t gmt_create = 0;
  int64_t gmt_modified = 0;
  type_filter.is_filter_enabled_ = true;
  ASSERT_EQ(OB_SUCCESS, type_filter.types_.push_back(ObSharedGCTaskType::LS_CKPT_GC));
  ASSERT_EQ(OB_SUCCESS, task_iter.init(TENANT_ID, OB_ALL_SHARED_GC_TASK_TNAME,
      type_filter, round_id_filter, sql_proxy()));
  while (OB_SUCCESS == (ret = task_iter.get_next(
      task_allocator, inserted_task, gmt_create, gmt_modified))) {
    ObSharedGCLSCheckpointInfo info;
    ASSERT_EQ(OB_SUCCESS, info.from_value(inserted_task.value_));
    if (info.ls_id_ == value.ls_id_ && info.meta_scn_ == value.meta_scn_) {
      task_found = true;
      ASSERT_EQ(0, inserted_task.key_.round_id_);
      break;
    }
  }
  ASSERT_TRUE(task_found);
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::delete_task(inserted_task.key_, sql_proxy()));
}

TEST(SharedGCTaskDefine, mark_sweep_gc_stat_types)
{
  ASSERT_TRUE(is_valid_shared_gc_task_type(ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT));
  ASSERT_TRUE(is_valid_shared_gc_task_type(ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT));
  ASSERT_TRUE(is_mark_sweep_gc_in_table_stat_type(ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT));
  ASSERT_TRUE(is_mark_sweep_gc_object_storage_stat_type(
      ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT));
  ASSERT_TRUE(is_mark_sweep_gc_stat_type(ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT));
  ASSERT_TRUE(is_mark_sweep_gc_stat_type(ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT));
  ASSERT_FALSE(is_mark_sweep_gc_type(ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT));
  ASSERT_FALSE(is_shared_data_gc_task_type(ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT));
  ASSERT_FALSE(is_shared_data_gc_task_type(ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT));
  ASSERT_STREQ("MARK_SWEEP_GC_IN_TABLE_STAT",
      ob_shared_gc_task_type_to_string(ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT));
  ASSERT_STREQ("MARK_SWEEP_GC_OBJECT_STORAGE_STAT",
      ob_shared_gc_task_type_to_string(ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT));
  ASSERT_EQ(ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT,
      ob_string_to_shared_gc_task_type(ObString::make_string("MARK_SWEEP_GC_IN_TABLE_STAT")));
  ASSERT_EQ(ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT,
      ob_string_to_shared_gc_task_type(ObString::make_string("MARK_SWEEP_GC_OBJECT_STORAGE_STAT")));
}

TEST(SharedGCTaskDefine, mark_sweep_gc_stat_value_validation)
{
  const ObSharedGCTaskType stat_types[] = {
      ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT,
      ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT};
  const char *valid_values[] = {"1", "10", "9223372036854775807"};
  const char *invalid_values[] = {
      "", "0", "00", "01", "+1", "-1", " 1", "1 ", "1\n", "1\r\n", "1.0",
      "1e2", "bad", "9223372036854775808"};
  ObSharedGCTask task;
  task.key_.tenant_id_ = OB_SYS_TENANT_ID;
  task.key_.round_id_ = 1;
  task.key_.task_id_ = 1;

  for (int64_t type_idx = 0; type_idx < ARRAYSIZEOF(stat_types); ++type_idx) {
    task.key_.type_ = stat_types[type_idx];
    for (int64_t value_idx = 0; value_idx < ARRAYSIZEOF(valid_values); ++value_idx) {
      task.value_ = ObString::make_string(valid_values[value_idx]);
      ASSERT_TRUE(task.is_valid());
    }
    for (int64_t value_idx = 0; value_idx < ARRAYSIZEOF(invalid_values); ++value_idx) {
      task.value_ = ObString::make_string(invalid_values[value_idx]);
      ASSERT_FALSE(task.is_valid());
    }
  }
}

TEST_F(TestSharedGCTaskTableOperator, mark_sweep_gc_stat_type_filter_sql)
{
  const ObSharedGCTaskType stat_types[] = {
      ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT,
      ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT};
  const char *expected_filters[] = {"type IN (8)", "type IN (9)"};
  const char *unexpected_filters[] = {"type IN (9)", "type IN (8)"};

  for (int64_t i = 0; i < ARRAYSIZEOF(stat_types); ++i) {
    ObSharedGCTaskTypeFilter type_filter;
    ObSharedGCTaskRoundIdFilter round_id_filter;
    ObSharedGCTaskTableFullIterator iter;
    ObSqlString sql;
    type_filter.is_filter_enabled_ = true;
    ASSERT_EQ(OB_SUCCESS, type_filter.types_.push_back(stat_types[i]));
    ASSERT_EQ(OB_SUCCESS, iter.init(
        TENANT_ID,
        OB_ALL_SHARED_GC_TASK_HISTORY_TNAME,
        type_filter,
        round_id_filter,
        sql_proxy()));
    ASSERT_EQ(OB_SUCCESS, iter.build_select_sql_(sql));
    ASSERT_NE(nullptr, STRSTR(sql.ptr(), expected_filters[i]));
    ASSERT_EQ(nullptr, STRSTR(sql.ptr(), unexpected_filters[i]));
    ASSERT_NE(nullptr, STRSTR(sql.ptr(), "ORDER BY type, round_id, task_id"));
  }
}

// =====================================================================
// __all_shared_gc_task tests
// =====================================================================

TEST_F(TestSharedGCTaskTableOperator, delete_task_operator_and_iterators)
{
  const uint64_t target_tenant_id = 8000000000000001002ULL;
  const uint64_t target_server_id = 900001;
  const int64_t delete_ts = ObTimeUtility::current_time();
  const ObSharedGCTaskKey tenant_key(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::DELETE_TENANT,
      0 /* round_id */, static_cast<int64_t>(target_tenant_id));
  const ObSharedGCTaskKey server_key(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::DELETE_SERVER,
      0 /* round_id */, static_cast<int64_t>(target_server_id));

  // Remove rows left by an interrupted previous run.
  int cleanup_ret = ObSharedGCTaskTableOperator::delete_task(tenant_key, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);
  cleanup_ret = ObSharedGCTaskTableOperator::delete_task(server_key, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);

  ObAddr server_addr;
  ASSERT_TRUE(server_addr.set_ip_addr("127.0.0.89", 2882));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskUtil::generate_delete_tenant_task(
      target_tenant_id, delete_ts, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskUtil::generate_delete_server_task(
      target_server_id, server_addr, delete_ts, sql_proxy()));

  // 每个专用 Iterator 只能返回自身负责的任务类型。测试按确定性 key 查找任务，
  // 避免无关行影响断言。
  {
    int ret = OB_SUCCESS;
    bool found = false;
    ObArenaAllocator allocator("DelTenantIt");
    ObDeleteTenantTaskIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(sql_proxy()));
    ObSharedGCTask task;
    ObDeleteTenantTaskValue value;
    while (OB_SUCCESS == (ret = iter.get_next(allocator, task, value))) {
      ASSERT_EQ(OB_SYS_TENANT_ID, task.key_.tenant_id_);
      ASSERT_EQ(ObSharedGCTaskType::DELETE_TENANT, task.key_.type_);
      ASSERT_EQ(0, task.key_.round_id_);
      if (tenant_key.task_id_ == task.key_.task_id_) {
        found = true;
        ASSERT_EQ(target_tenant_id, value.tenant_id_);
        ASSERT_EQ(delete_ts, value.delete_ts_);
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_TRUE(found);
  }
  {
    int ret = OB_SUCCESS;
    bool found = false;
    ObArenaAllocator allocator("DelServerIt");
    ObDeleteServerTaskIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(sql_proxy()));
    ObSharedGCTask task;
    ObDeleteServerTaskValue value;
    while (OB_SUCCESS == (ret = iter.get_next(allocator, task, value))) {
      ASSERT_EQ(OB_SYS_TENANT_ID, task.key_.tenant_id_);
      ASSERT_EQ(ObSharedGCTaskType::DELETE_SERVER, task.key_.type_);
      ASSERT_EQ(0, task.key_.round_id_);
      if (server_key.task_id_ == task.key_.task_id_) {
        found = true;
        ASSERT_EQ(target_server_id, value.server_id_);
        ASSERT_TRUE(server_addr == value.server_addr_);
        ASSERT_EQ(delete_ts, value.delete_ts_);
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_TRUE(found);
  }

  // 可读虚拟表必须支持两种新增任务类型并解码其二进制 payload，不能将其视为
  // 未知类型而中止扫描。
  {
    // simple-server 测试套未以 shared-storage 模式启动，而虚拟表在非该模式下
    // 会按设计返回空结果。
    ObServerModeGuard mode_guard(observer::ObServerMode::SHARED_STORAGE_MODE);
    int ret = OB_SUCCESS;
    int64_t row_count = 0;
    bool tenant_found = false;
    bool server_found = false;
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT type, task_id, value FROM oceanbase.%s "
        "WHERE tenant_id = %lu AND round_id = 0 "
        "AND ((type = 'DELETE_TENANT' AND task_id = %ld) "
        "OR (type = 'DELETE_SERVER' AND task_id = %ld))",
        OB_ALL_VIRTUAL_SHARED_GC_TASK_TNAME,
        OB_SYS_TENANT_ID,
        tenant_key.task_id_,
        server_key.task_id_));
    HEAP_VAR(ObMySQLProxy::MySQLResult, result) {
      sqlclient::ObMySQLResult *r = nullptr;
      ASSERT_EQ(OB_SUCCESS, sql_proxy().read(result, OB_SYS_TENANT_ID, sql.ptr()));
      ASSERT_NE(nullptr, r = result.get_result());
      while (OB_SUCC(ret = r->next())) {
        ObString type;
        ObString readable_value;
        int64_t task_id = 0;
        ASSERT_EQ(OB_SUCCESS, r->get_varchar("type", type));
        ASSERT_EQ(OB_SUCCESS, r->get_int("task_id", task_id));
        ASSERT_EQ(OB_SUCCESS, r->get_varchar("value", readable_value));
        ASSERT_FALSE(readable_value.empty());
        if (ObString::make_string("DELETE_TENANT") == type) {
          ASSERT_EQ(tenant_key.task_id_, task_id);
          tenant_found = true;
        } else if (ObString::make_string("DELETE_SERVER") == type) {
          ASSERT_EQ(server_key.task_id_, task_id);
          server_found = true;
        } else {
          FAIL() << "unexpected shared gc task type";
        }
        ++row_count;
      }
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ASSERT_EQ(2, row_count);
    ASSERT_TRUE(tenant_found);
    ASSERT_TRUE(server_found);
  }

  // shared-data 范围 Iterator 必须继续拒绝 LS_META 和两种删除任务类型，
  // 这些类型均由各自的专用 Iterator 处理。
  const ObSSGCTaskIdRange range(0, INT64_MAX);
  ObSharedGCTaskTableRangeIterator range_iter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, range_iter.init(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::LS_META_GC, 0, range, sql_proxy()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, range_iter.init(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::DELETE_TENANT, 0, range, sql_proxy()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, range_iter.init(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::DELETE_SERVER, 0, range, sql_proxy()));

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(tenant_key, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(server_key, sql_proxy()));
}

TEST_F(TestSharedGCTaskTableOperator, delete_tenant_iterator_rejects_mismatched_id)
{
  const uint64_t value_tenant_id = 8000000000000001002ULL;
  const uint64_t key_tenant_id = 8000000000000001004ULL;
  const ObSharedGCTaskKey key(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::DELETE_TENANT,
      0 /* round_id */, static_cast<int64_t>(key_tenant_id));
  const int cleanup_ret = ObSharedGCTaskTableOperator::delete_task(key, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);

  ObArenaAllocator value_allocator("DelTenantBad");
  ObDeleteTenantTaskValue value;
  value.tenant_id_ = value_tenant_id;
  value.delete_ts_ = ObTimeUtility::current_time();
  ObString serialized_value;
  ASSERT_EQ(OB_SUCCESS, value.to_value(value_allocator, serialized_value));
  ObSharedGCTask invalid_task;
  invalid_task.key_ = key;
  invalid_task.value_ = serialized_value;
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::insert_task(invalid_task, sql_proxy()));

  int ret = OB_SUCCESS;
  bool rejected = false;
  ObArenaAllocator iter_allocator("DelTenantBadIt");
  ObDeleteTenantTaskIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(sql_proxy()));
  ObSharedGCTask task;
  ObDeleteTenantTaskValue decoded_value;
  while (OB_SUCCESS == (ret = iter.get_next(iter_allocator, task, decoded_value))) {
  }
  if (OB_INVALID_DATA == ret && key.task_id_ == task.key_.task_id_) {
    rejected = true;
  }
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(key, sql_proxy()));
  ASSERT_TRUE(rejected);
}

// INSERT + iterator scan round trip. Insert 3 rows under the same (type, round_id)
// and verify they come back in (round_id, task_id) order.
TEST_F(TestSharedGCTaskTableOperator, insert_and_get_tasks_paginated)
{
  ObArenaAllocator allocator;
  const int64_t round_id = 1000;

  // Insert 3 rows with task_id 1/2/3
  for (int64_t tid = 1; tid <= 3; ++tid) {
    ObSharedGCTask task;
    ObSharedGCFailInfo info;
    ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, round_id, tid, task, info));
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));
  }

  // First two tasks returned in (round_id, task_id) order
  const ObSSGCTaskIdRange iter_range(0, INT64_MAX);
  ObSharedGCTaskTableRangeIterator iter1;
  ASSERT_EQ(OB_SUCCESS, iter1.init(TENANT_ID, ObSharedGCTaskType::FAIL_GC,
                                    round_id, iter_range, sql_proxy()));
  ObSharedGCTask first_task, second_task;
  ASSERT_EQ(OB_SUCCESS, iter1.get_next(allocator, first_task));
  ASSERT_EQ(OB_SUCCESS, iter1.get_next(allocator, second_task));
  ASSERT_EQ(1, first_task.key_.task_id_);
  ASSERT_EQ(2, second_task.key_.task_id_);

  // Full scan: all 3 tasks returned
  ObSharedGCTaskTableRangeIterator iter2;
  ASSERT_EQ(OB_SUCCESS, iter2.init(TENANT_ID, ObSharedGCTaskType::FAIL_GC,
                                    round_id, iter_range, sql_proxy()));
  ObSharedGCTask task;
  int64_t total = 0;
  while (OB_SUCCESS == iter2.get_next(allocator, task)) { ++total; }
  ASSERT_EQ(3, total);

  // Round-trip check: deserialize Fail value and compare key fields
  ObSharedGCFailInfo decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.from_value(first_task.value_));
  ASSERT_TRUE(blocksstable::ObStorageObjectType::SHARED_TABLET_META == decoded.meta_type_);
  ASSERT_EQ(12345, decoded.op_id_);
  ASSERT_EQ(share::ObLSID(1001), decoded.ls_id_);
  ASSERT_EQ(ObTabletID(2001), decoded.tablet_id_);

  // Cleanup
  for (int64_t tid = 1; tid <= 3; ++tid) {
    ObSharedGCTaskKey key(TENANT_ID, ObSharedGCTaskType::FAIL_GC, round_id, tid);
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(key, sql_proxy()));
  }
}

// UPDATE server_id / trace_id: setting and clearing them. Read back via
// iterator and verify NULL <-> value transitions.
TEST_F(TestSharedGCTaskTableOperator, update_server_id_and_trace_id)
{
  ObArenaAllocator allocator;
  const int64_t round_id = 1300;
  const int64_t task_id = 1;

  ObSharedGCTask task; ObSharedGCFailInfo info;
  ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, round_id, task_id, task, info));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));

  // Set server_id + trace_id
  const ObString trace = ObString::make_string("trc-abc-123");
  const ObString trace_retry = ObString::make_string("trc-retry-456");
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
      task.key_, 7777 /*server_id*/, trace, sql_proxy()));
  int64_t first_modified_us = 0;
  int64_t second_modified_us = 0;
  ASSERT_EQ(OB_SUCCESS, get_active_task_modified_us(task.key_, sql_proxy(), first_modified_us));
  ob_usleep(10 * 1000);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
      task.key_, 7777 /*server_id*/, trace_retry, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, get_active_task_modified_us(task.key_, sql_proxy(), second_modified_us));
  ASSERT_GT(second_modified_us, first_modified_us);

  // Read back and verify
  {
    const ObSSGCTaskIdRange iter_range(0, INT64_MAX);
    ObSharedGCTaskTableRangeIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(TENANT_ID, task.key_.type_, round_id, iter_range, sql_proxy()));
    ObSharedGCTask row;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, row));
    ASSERT_EQ(7777, row.server_id_);
    ASSERT_EQ(trace_retry, row.trace_id_);
  }

  // Clear trace_id and server_id (back to NULL)
  ObString empty_trace;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
      task.key_, ObSharedGCTask::INVALID_SERVER_ID, empty_trace, sql_proxy()));

  {
    const ObSSGCTaskIdRange iter_range(0, INT64_MAX);
    ObSharedGCTaskTableRangeIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(TENANT_ID, task.key_.type_, round_id, iter_range, sql_proxy()));
    ObSharedGCTask row;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, row));
    ASSERT_EQ(ObSharedGCTask::INVALID_SERVER_ID, row.server_id_);
    ASSERT_TRUE(row.trace_id_.empty());
  }

  // active 行不存在不是认领成功，调用方必须跳过 Step C。
  ObSharedGCTaskKey absent_key(TENANT_ID, ObSharedGCTaskType::FAIL_GC,
                               round_id, 999);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
      absent_key, 7777, trace, sql_proxy()));

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(task.key_, sql_proxy()));
}

// UPDATE extra_info: simple round trip. Only extra_info changes; other columns
// untouched.
TEST_F(TestSharedGCTaskTableOperator, update_extra_info)
{
  ObArenaAllocator allocator;
  const int64_t round_id = 1400;
  const int64_t task_id = 1;

  ObSharedGCTask task; ObSharedGCFailInfo info;
  ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, round_id, task_id, task, info));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));

  const ObString new_diag = ObString::make_string("phase-2 retry, gc=ok, parallel=4, step=3");
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::update_extra_info(
      task.key_, new_diag, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::update_extra_info(
      task.key_, new_diag, sql_proxy()));

  const ObSSGCTaskIdRange iter_range(0, INT64_MAX);
  ObSharedGCTaskTableRangeIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(TENANT_ID, task.key_.type_, round_id, iter_range, sql_proxy()));
  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, row));
  ASSERT_EQ(new_diag, row.extra_info_);

  // value column unchanged: deserialize and check op_id round-trips
  ObSharedGCFailInfo decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.from_value(row.value_));
  ASSERT_EQ(12345, decoded.op_id_);

  // Non-existing key
  ObSharedGCTaskKey absent_key(TENANT_ID, ObSharedGCTaskType::FAIL_GC,
                               round_id, 999);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            ObSharedGCTaskTableOperator::update_extra_info(absent_key, new_diag, sql_proxy()));

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(task.key_, sql_proxy()));
}

// DELETE 不存在的 key 会返回 OB_ENTRY_NOT_EXIST（operator 把 affected_rows==0
// 映射为 not-found 错误）。容忍语义仅在 delete_task_and_insert_history 内做。
TEST_F(TestSharedGCTaskTableOperator, delete_non_existing)
{
  ObSharedGCTaskKey absent_key(TENANT_ID, ObSharedGCTaskType::FAIL_GC,
                               1500, 1);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            ObSharedGCTaskTableOperator::delete_task(absent_key, sql_proxy()));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            ObSharedGCTaskTableOperator::delete_history(absent_key, sql_proxy()));
}

// Batch INSERT N rows under the same tenant_id.
TEST_F(TestSharedGCTaskTableOperator, batch_insert_tasks)
{
  ObArenaAllocator allocator;
  const int64_t round_id = 1600;
  const int64_t N = 5;

  ObArray<ObSharedGCTask> tasks;
  for (int64_t tid = 1; tid <= N; ++tid) {
    ObSharedGCTask task; ObSharedGCFailInfo info;
    ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, round_id, tid, task, info));
    ASSERT_EQ(OB_SUCCESS, tasks.push_back(task));
  }
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_insert_tasks(
      TENANT_ID, tasks, sql_proxy()));

  {
    const ObSSGCTaskIdRange iter_range(0, INT64_MAX);
    ObSharedGCTaskTableRangeIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(TENANT_ID, ObSharedGCTaskType::FAIL_GC,
                                     round_id, iter_range, sql_proxy()));
    ObSharedGCTask t;
    int64_t count = 0;
    while (OB_SUCCESS == iter.get_next(allocator, t)) { ++count; }
    ASSERT_EQ(N, count);
  }

  // tenant_id mismatch in batch_insert is rejected
  ObArray<ObSharedGCTask> mismatched;
  {
    ObSharedGCTask t; ObSharedGCFailInfo info;
    ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, round_id, N + 1, t, info));
    t.key_.tenant_id_ = 1234;  // not equal to TENANT_ID
    ASSERT_EQ(OB_SUCCESS, mismatched.push_back(t));
  }
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObSharedGCTaskTableOperator::batch_insert_tasks(TENANT_ID, mismatched, sql_proxy()));

  // Cleanup
  for (int64_t tid = 1; tid <= N; ++tid) {
    ObSharedGCTaskKey key(TENANT_ID, ObSharedGCTaskType::FAIL_GC, round_id, tid);
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(key, sql_proxy()));
  }
}

// MarkSweep value: serialize a list of macro_ids, store as task.value_, read
// back via iterator, and round-trip through ObSharedGCMacroIdList::from_value.
TEST_F(TestSharedGCTaskTableOperator, mark_sweep_value_round_trip)
{
  ObArenaAllocator allocator;
  const int64_t round_id = 1700;
  const int64_t task_id = 1;

  ObSharedGCMacroIdList list;
  ObString value;
  ASSERT_EQ(OB_SUCCESS, build_mark_sweep_value(allocator, 50, list, value));

  ObSharedGCTask task;
  task.key_ = ObSharedGCTaskKey(TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC,
                                round_id, task_id);
  task.value_ = value;
  task.extra_info_ = ObString::make_string("");
  task.server_id_ = ObSharedGCTask::INVALID_SERVER_ID;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));

  const ObSSGCTaskIdRange iter_range(0, INT64_MAX);
  ObSharedGCTaskTableRangeIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC,
                                   round_id, iter_range, sql_proxy()));
  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, row));

  ObSharedGCMacroIdList decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.from_value(row.value_));
  ASSERT_EQ(50, decoded.count());
  for (int64_t i = 0; i < 50; ++i) {
    ASSERT_EQ(static_cast<int64_t>(10000 + i),
              decoded.at(i).second_id());
  }

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(task.key_, sql_proxy()));
}

// =====================================================================
// __all_shared_gc_task_history tests
// =====================================================================

// INSERT into history, GET by primary key, DELETE.
TEST_F(TestSharedGCTaskTableOperator, history_crud)
{
  ObArenaAllocator allocator;
  const int64_t round_id = 1800;
  const int64_t task_id = 1;

  ObSharedGCTask task; ObSharedGCFailInfo info;
  ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, round_id, task_id, task, info));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_history(task, sql_proxy()));

  ObSharedGCTask read_back;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      task.key_, allocator, sql_proxy(), read_back));
  ASSERT_EQ(task.key_.tenant_id_, read_back.key_.tenant_id_);
  ASSERT_EQ(task.key_.type_, read_back.key_.type_);
  ASSERT_EQ(task.key_.round_id_, read_back.key_.round_id_);
  ASSERT_EQ(task.key_.task_id_, read_back.key_.task_id_);
  ASSERT_EQ(task.extra_info_, read_back.extra_info_);

  ObSharedGCFailInfo decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.from_value(read_back.value_));
  ASSERT_EQ(info.op_id_, decoded.op_id_);

  // get_history on non-existing key returns OB_ENTRY_NOT_EXIST
  ObSharedGCTaskKey absent_key(TENANT_ID, ObSharedGCTaskType::FAIL_GC,
                               round_id, 999);
  ObSharedGCTask absent_read;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            ObSharedGCTaskTableOperator::get_history(absent_key, allocator,
                                                     sql_proxy(), absent_read));

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_history(task.key_, sql_proxy()));
  // gone after delete
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            ObSharedGCTaskTableOperator::get_history(task.key_, allocator,
                                                     sql_proxy(), read_back));
}

TEST_F(TestSharedGCTaskTableOperator, mark_sweep_gc_stat_history_rejects_invalid_value)
{
  const ObSharedGCTaskType stat_types[] = {
      ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT,
      ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT};
  ObSharedGCTask task;
  task.key_.tenant_id_ = TENANT_ID;
  task.key_.round_id_ = 1808;
  task.value_ = ObString::make_string("12x");

  for (int64_t i = 0; i < ARRAYSIZEOF(stat_types); ++i) {
    task.key_.type_ = stat_types[i];
    task.key_.task_id_ = i + 1;
    ASSERT_EQ(OB_INVALID_ARGUMENT, ObSharedGCTaskTableOperator::insert_history(task, sql_proxy()));
  }
}

TEST_F(TestSharedGCTaskTableOperator, mark_sweep_gc_history_stats_data_version_gate)
{
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  const uint64_t saved_cluster_version = GET_MIN_CLUSTER_VERSION();
  const uint64_t saved_data_version = ATOMIC_LOAD(&cluster_version.data_version_);
  const uint64_t saved_mock_data_version = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  uint64_t actual_data_version = 0;
  DEFER({
    cluster_version.update_cluster_version(saved_cluster_version);
    cluster_version.update_data_version(saved_data_version);
    ODV_MGR.set_mock_data_version(saved_mock_data_version);
  });
  cluster_version.update_data_version(0);
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_2_1);
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(TENANT_ID, actual_data_version));
  ASSERT_EQ(DATA_VERSION_4_6_2_1, actual_data_version);

  ObArenaAllocator allocator("MSGCStatGate");
  const int64_t round_id = 1809;
  const int64_t task_id = 1;
  ObSharedGCMacroIdList macro_id_list;
  ObString value;
  ASSERT_EQ(OB_SUCCESS, build_mark_sweep_stat_value(allocator, 1, 0, macro_id_list, value));
  ObSharedGCTask task;
  task.key_ = ObSharedGCTaskKey(TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC, round_id, task_id);
  task.value_ = value;
  const ObSharedGCTaskKey stat_key(
      TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT,
      round_id, task_id);

  int cleanup_ret = ObSharedGCTaskTableOperator::delete_task(task.key_, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);
  cleanup_ret = ObSharedGCTaskTableOperator::delete_history(task.key_, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);
  cleanup_ret = ObSharedGCTaskTableOperator::delete_history(stat_key, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));
  ObArray<ObSharedGCTask> tasks;
  ASSERT_EQ(OB_SUCCESS, tasks.push_back(task));
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::batch_delete_tasks_and_insert_histories(TENANT_ID, tasks));

  ObSharedGCTask history;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      task.key_, allocator, sql_proxy(), history));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObSharedGCTaskTableOperator::get_history(
      stat_key, allocator, sql_proxy(), history));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_history(task.key_, sql_proxy()));
}

TEST_F(TestSharedGCTaskTableOperator, mark_sweep_gc_history_stats)
{
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  const uint64_t saved_cluster_version = GET_MIN_CLUSTER_VERSION();
  const uint64_t saved_data_version = ATOMIC_LOAD(&cluster_version.data_version_);
  const uint64_t saved_mock_data_version = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  uint64_t actual_data_version = 0;
  ASSERT_TRUE(share::ObUpgradeChecker::check_data_version_exist(DATA_VERSION_4_6_3_0));
  DEFER({
    cluster_version.update_cluster_version(saved_cluster_version);
    cluster_version.update_data_version(saved_data_version);
    ODV_MGR.set_mock_data_version(saved_mock_data_version);
  });
  cluster_version.update_data_version(0);
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_0);
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(TENANT_ID, actual_data_version));
  ASSERT_EQ(DATA_VERSION_4_6_3_0, actual_data_version);

  ObArenaAllocator allocator("MSGCStat");
  const int64_t round_id = 1810;
  const int64_t task_id = 1;
  ObSharedGCMacroIdList macro_id_list;
  ObString value;
  ASSERT_EQ(OB_SUCCESS, build_mark_sweep_stat_value(allocator, 3, 2, macro_id_list, value));
  ObSharedGCTask task;
  task.key_ = ObSharedGCTaskKey(TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC, round_id, task_id);
  task.value_ = value;
  task.server_id_ = ObSharedGCTask::INVALID_SERVER_ID;
  const ObSharedGCTaskKey in_table_stat_key(
      TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT,
      round_id, task_id);
  const ObSharedGCTaskKey object_storage_stat_key(
      TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT,
      round_id, task_id);

  int cleanup_ret = ObSharedGCTaskTableOperator::delete_task(task.key_, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);
  cleanup_ret = ObSharedGCTaskTableOperator::delete_history(task.key_, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);
  cleanup_ret = ObSharedGCTaskTableOperator::delete_history(in_table_stat_key, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);
  cleanup_ret = ObSharedGCTaskTableOperator::delete_history(object_storage_stat_key, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));
  ObArray<ObSharedGCTask> tasks;
  ASSERT_EQ(OB_SUCCESS, tasks.push_back(task));
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::batch_delete_tasks_and_insert_histories(TENANT_ID, tasks));
  // 重放同一批任务，统计记录不能重复。
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::batch_delete_tasks_and_insert_histories(TENANT_ID, tasks));

  ObSharedGCTask original_history;
  ObSharedGCTask in_table_stat;
  ObSharedGCTask object_storage_stat;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      task.key_, allocator, sql_proxy(), original_history));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      in_table_stat_key, allocator, sql_proxy(), in_table_stat));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      object_storage_stat_key, allocator, sql_proxy(), object_storage_stat));
  ASSERT_EQ(ObString::make_string("3"), in_table_stat.value_);
  // SYS 租户的 3 个 in-table 元数据宏块同时删除对象存储副本。
  ASSERT_EQ(ObString::make_string("5"), object_storage_stat.value_);
  ObSharedGCMacroIdList decoded_list;
  ASSERT_EQ(OB_SUCCESS, decoded_list.from_value(original_history.value_));
  ASSERT_EQ(5, decoded_list.count());

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_history(task.key_, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_history(
      in_table_stat_key, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_history(
      object_storage_stat_key, sql_proxy()));
}

TEST_F(TestSharedGCTaskTableOperator,
       mark_sweep_gc_stat_virtual_history_single_row_and_type_pushdown)
{
  const int64_t round_id = 1811;
  const int64_t task_id = 1;
  const ObSharedGCTaskType stat_types[] = {
      ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT,
      ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT};
  const char *stat_type_names[] = {
      "MARK_SWEEP_GC_IN_TABLE_STAT",
      "MARK_SWEEP_GC_OBJECT_STORAGE_STAT"};
  // 分别覆盖 utf8mb4_general_ci 的大小写不敏感和 PAD SPACE 等值条件。
  const char *stat_type_filter_names[] = {
      "mark_sweep_gc_in_table_stat",
      "MARK_SWEEP_GC_OBJECT_STORAGE_STAT   "};
  const char *stat_values[] = {"3", "2"};
  ObSharedGCTask raw_task;
  ObSharedGCTask stat_tasks[ARRAYSIZEOF(stat_types)];
  ObSharedGCMacroIdList decode_probe;
  ObSqlString sql;
  ObString type;
  ObString value;
  int ret = OB_SUCCESS;
  int cleanup_ret = OB_SUCCESS;
  int64_t row_count = 0;
  int64_t stat_count = 0;
  int64_t tenant_id = 0;
  int64_t result_round_id = 0;
  int64_t result_task_id = 0;
  ObServerModeGuard mode_guard(observer::ObServerMode::SHARED_STORAGE_MODE);
  raw_task.key_ = ObSharedGCTaskKey(
      TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC, round_id, task_id);
  raw_task.value_ = ObString::make_string("bad_macro_id_list");
  for (int64_t i = 0; i < ARRAYSIZEOF(stat_types); ++i) {
    stat_tasks[i].key_ = ObSharedGCTaskKey(TENANT_ID, stat_types[i], round_id, task_id);
    stat_tasks[i].value_ = ObString::make_string(stat_values[i]);
  }
  cleanup_ret = ObSharedGCTaskTableOperator::delete_history(raw_task.key_, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);
  for (int64_t i = 0; i < ARRAYSIZEOF(stat_types); ++i) {
    cleanup_ret = ObSharedGCTaskTableOperator::delete_history(stat_tasks[i].key_, sql_proxy());
    ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);
  }
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_history(raw_task, sql_proxy()));
  for (int64_t i = 0; i < ARRAYSIZEOF(stat_types); ++i) {
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_history(stat_tasks[i], sql_proxy()));
    decode_probe.reset();
    ASSERT_NE(OB_SUCCESS, decode_probe.from_value(stat_tasks[i].value_));
  }
  for (int64_t i = 0; i < ARRAYSIZEOF(stat_types); ++i) {
    ret = OB_SUCCESS;
    row_count = 0;
    tenant_id = 0;
    result_round_id = 0;
    result_task_id = 0;
    type.reset();
    value.reset();
    sql.reset();
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT tenant_id, type, round_id, task_id, value "
        "FROM oceanbase.%s WHERE type = '%s' "
        "AND round_id = %ld AND task_id = %ld",
        OB_ALL_VIRTUAL_SHARED_GC_TASK_HISTORY_TNAME,
        stat_type_filter_names[i],
        round_id,
        task_id));
    HEAP_VAR(ObMySQLProxy::MySQLResult, result) {
      sqlclient::ObMySQLResult *r = nullptr;
      ASSERT_EQ(OB_SUCCESS, sql_proxy().read(result, OB_SYS_TENANT_ID, sql.ptr()));
      ASSERT_NE(nullptr, r = result.get_result());
      while (OB_SUCC(ret = r->next())) {
        ASSERT_EQ(OB_SUCCESS, r->get_int("tenant_id", tenant_id));
        ASSERT_EQ(OB_SUCCESS, r->get_varchar("type", type));
        ASSERT_EQ(OB_SUCCESS, r->get_int("round_id", result_round_id));
        ASSERT_EQ(OB_SUCCESS, r->get_int("task_id", result_task_id));
        ASSERT_EQ(OB_SUCCESS, r->get_varchar("value", value));
        ASSERT_EQ(TENANT_ID, tenant_id);
        ASSERT_EQ(ObString::make_string(stat_type_names[i]), type);
        ASSERT_EQ(round_id, result_round_id);
        ASSERT_EQ(task_id, result_task_id);
        ASSERT_EQ(ObString::make_string(stat_values[i]), value);
        ++row_count;
      }
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ASSERT_EQ(1, row_count);
  }
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT COUNT(*) AS stat_count FROM oceanbase.%s "
      "WHERE type IN ('MARK_SWEEP_GC_IN_TABLE_STAT', "
      "'MARK_SWEEP_GC_OBJECT_STORAGE_STAT') "
      "AND round_id = %ld AND task_id = %ld",
      OB_ALL_VIRTUAL_SHARED_GC_TASK_HISTORY_TNAME,
      round_id,
      task_id));
  ret = OB_SUCCESS;
  HEAP_VAR(ObMySQLProxy::MySQLResult, result) {
    sqlclient::ObMySQLResult *r = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy().read(result, OB_SYS_TENANT_ID, sql.ptr()));
    ASSERT_NE(nullptr, r = result.get_result());
    ASSERT_EQ(OB_SUCCESS, r->next());
    ASSERT_EQ(OB_SUCCESS, r->get_int("stat_count", stat_count));
    ASSERT_EQ(OB_ITER_END, r->next());
  }
  ASSERT_EQ(ARRAYSIZEOF(stat_types), stat_count);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_history(raw_task.key_, sql_proxy()));
  for (int64_t i = 0; i < ARRAYSIZEOF(stat_types); ++i) {
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_history(
        stat_tasks[i].key_, sql_proxy()));
  }
}

// Records actual write sizes without retaining SQL buffers between calls.
class ObHistoryBatchSQLClient : public ObMySQLProxy
{
public:
  explicit ObHistoryBatchSQLClient(const int64_t fail_write_idx)
      : fail_write_idx_(fail_write_idx), write_count_(0), max_sql_length_(0),
        client_(nullptr), peak_memory_(0), retained_memory_(0) {}
  virtual ~ObHistoryBatchSQLClient() {}
  using ObMySQLProxy::write;
  int write(
      const uint64_t tenant_id,
      const char *sql,
      const int32_t group_id,
      int64_t &affected_rows) override;
  int64_t get_write_count() const { return write_count_; }
  int64_t get_max_sql_length() const { return max_sql_length_; }
  int64_t get_peak_memory() const { return peak_memory_; }
  int64_t get_retained_memory() const { return retained_memory_; }
  void set_client(ObISQLClient &client) { client_ = &client; }
private:
  int64_t fail_write_idx_;
  int64_t write_count_;
  int64_t max_sql_length_;
  ObISQLClient *client_;
  int64_t peak_memory_;
  int64_t retained_memory_;
  DISALLOW_COPY_AND_ASSIGN(ObHistoryBatchSQLClient);
};

int ObHistoryBatchSQLClient::write(
    const uint64_t tenant_id,
    const char *sql,
    const int32_t group_id,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ++write_count_;
    max_sql_length_ = MAX(max_sql_length_, static_cast<int64_t>(STRLEN(sql)));
    if (write_count_ == fail_write_idx_) {
      ret = OB_TIMEOUT;
    } else if (OB_NOT_NULL(client_)) {
      int64_t statement_peak = 0;
      observer::ObProcessMallocCallback callback(0, statement_peak);
      lib::ObMallocCallbackGuard guard(callback);
      ret = client_->write(tenant_id, sql, group_id, affected_rows);
      peak_memory_ = MAX(peak_memory_, statement_peak);
      retained_memory_ += callback.get_cur_used();
    }
  }
  return ret;
}

int TestSharedGCTaskTableOperator::insert_histories_with_memory(
    const ObSharedGCTaskIArray &tasks,
    int64_t &peak_memory,
    int64_t &retained_memory,
    int64_t &sql_peak_memory,
    int64_t &sql_retained_memory,
    int64_t &write_count)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObMySQLTransaction trans;
  ObHistoryBatchSQLClient client(0/*fail_write_idx*/);
  peak_memory = 0;
  retained_memory = 0;
  sql_peak_memory = 0;
  sql_retained_memory = 0;
  write_count = 0;
  client.set_client(trans);
  if (OB_FAIL(trans.start(&sql_proxy(), TENANT_ID))) {
    LOG_WARN("fail to start history memory test transaction", KR(ret));
  } else {
    {
      observer::ObProcessMallocCallback callback(0, peak_memory);
      lib::ObMallocCallbackGuard guard(callback);
      ret = ObSharedGCTaskTableOperator::batch_insert_histories_(
          TENANT_ID, tasks, client, affected_rows);
      retained_memory = callback.get_cur_used();
    }
    const int end_ret = trans.end(OB_SUCCESS == ret);
    if (OB_SUCC(ret)) {
      ret = end_ret;
    }
    if (OB_SUCC(ret) && (affected_rows != tasks.count()
        || client.get_max_sql_length()
            > ObSharedGCTaskTableOperator::MAX_HISTORY_BATCH_SQL_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  sql_peak_memory = client.get_peak_memory();
  sql_retained_memory = client.get_retained_memory();
  write_count = client.get_write_count();
  return ret;
}

int TestSharedGCTaskTableOperator::build_history_tasks(
    ObIAllocator &allocator,
    const int64_t round_id,
    const int64_t task_count,
    const int64_t value_size,
    ObSharedGCTaskIArray &tasks)
{
  int ret = OB_SUCCESS;
  char *value_buf = nullptr;
  ObSharedGCTask task;
  tasks.reset();
  if (round_id <= 0 || task_count <= 0 || value_size <= 0
      || value_size > ObSharedGCTask::MAX_VALUE_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(value_buf = static_cast<char *>(allocator.alloc(value_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMSET(value_buf, 'Z', value_size);
    for (int64_t i = 0; OB_SUCC(ret) && i < task_count; ++i) {
      task.reset();
      task.key_ = ObSharedGCTaskKey(TENANT_ID, ObSharedGCTaskType::FAIL_GC, round_id, i + 1);
      // Share immutable input to exclude test-data allocations from the measured SQL peak.
      task.value_.assign_ptr(value_buf, value_size);
      ret = tasks.push_back(task);
    }
  }
  return ret;
}

int TestSharedGCTaskTableOperator::get_history_count(
    const int64_t round_id,
    ObISQLClient &client,
    int64_t &row_count,
    int64_t &value_length)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  row_count = 0;
  value_length = 0;
  if (OB_FAIL(sql.assign_fmt(
      "SELECT COUNT(*) AS row_count, "
      "CAST(COALESCE(SUM(LENGTH(value)), 0) AS SIGNED) AS value_length "
      "FROM oceanbase.%s WHERE tenant_id = %lu AND round_id = %ld",
      OB_ALL_SHARED_GC_TASK_HISTORY_TNAME, TENANT_ID, round_id))) {
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(client.read(res, TENANT_ID, sql.ptr()))) {
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_int("row_count", row_count))) {
      } else if (OB_FAIL(result->get_int("value_length", value_length))) {
      }
    }
  }
  return ret;
}

TEST_F(TestSharedGCTaskTableOperator, history_batch_sql_memory)
{
  ObArenaAllocator allocator("GCHistMemTest");
  ObArray<ObSharedGCTask> tasks;
  ObHistoryBatchSQLClient client(0/*fail_write_idx*/);
  const int64_t task_count = 100;
  const int64_t value_size = 256 * 1024;
  const int64_t sql_limit = 2 * 1024 * 1024;
  const int64_t baseline_sql_size = 2 * task_count * value_size;
  const int64_t memory_budget = baseline_sql_size / 2;
  int64_t peak_memory = 0;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, build_history_tasks(allocator, 1812, task_count, value_size, tasks));
  {
    observer::ObProcessMallocCallback callback(0, peak_memory);
    lib::ObMallocCallbackGuard guard(callback);
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_insert_histories_(
        TENANT_ID, tasks, client, affected_rows));
  }
  // The unbatched hexadecimal values alone need baseline_sql_size bytes. Require
  // streaming construction to stay below half of that payload on instrumented builds.
  ASSERT_GE(client.get_write_count(), 34);
  ASSERT_LE(client.get_max_sql_length(), sql_limit);
#ifndef OB_USE_ASAN
  ASSERT_GT(peak_memory, value_size);
  ASSERT_LE(peak_memory, memory_budget);
  LOG_INFO("history batch caller memory", K(peak_memory), K(baseline_sql_size), K(memory_budget),
      "write_count", client.get_write_count(), "max_sql_length", client.get_max_sql_length());
#else
  // ASAN uses system malloc/free and bypasses the tenant allocator callback.
  LOG_INFO("history batch caller memory callback unsupported under ASAN",
      K(baseline_sql_size), K(memory_budget), "write_count", client.get_write_count(),
      "max_sql_length", client.get_max_sql_length());
#endif
}

TEST_F(TestSharedGCTaskTableOperator, history_batch_sql_row_limit_and_stats)
{
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  const uint64_t saved_cluster_version = GET_MIN_CLUSTER_VERSION();
  const uint64_t saved_data_version = ATOMIC_LOAD(&cluster_version.data_version_);
  const uint64_t saved_mock_data_version = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  ObArenaAllocator allocator("GCHistRowTest");
  ObArray<ObSharedGCTask> tasks;
  ObSharedGCMacroIdList macro_ids;
  ObString value;
  ObSqlString sql;
  int64_t task_idx = 0;
  int64_t row_count = 0;
  DEFER({
    cluster_version.update_cluster_version(saved_cluster_version);
    cluster_version.update_data_version(saved_data_version);
    ODV_MGR.set_mock_data_version(saved_mock_data_version);
  });
  cluster_version.update_data_version(0);
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_0);
  ASSERT_EQ(OB_SUCCESS, build_history_tasks(allocator, 1813, 100, 1, tasks));
  ASSERT_EQ(OB_SUCCESS, build_mark_sweep_stat_value(allocator, 1, 1, macro_ids, value));
  for (int64_t i = 0; i < tasks.count(); ++i) {
    if (0 == i % 2) {
      tasks.at(i).key_.type_ = ObSharedGCTaskType::MARK_SWEEP_GC;
      tasks.at(i).value_ = value;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(50, task_idx);
  ASSERT_EQ(100, row_count);
  ASSERT_LT(sql.length(), 2 * 1024 * 1024);
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(100, task_idx);
  ASSERT_EQ(100, row_count);
  // The old data version emits task rows only and should fit exactly one row-limited batch.
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_2_1);
  task_idx = 0;
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(100, task_idx);
  ASSERT_EQ(100, row_count);
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_0);
  for (int64_t i = 0; i < tasks.count(); ++i) {
    tasks.at(i).key_.type_ = ObSharedGCTaskType::MARK_SWEEP_GC;
    tasks.at(i).value_ = value;
  }
  task_idx = 0;
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(33, task_idx);
  ASSERT_EQ(99, row_count);
}

TEST_F(TestSharedGCTaskTableOperator, history_batch_sql_mixed_byte_limit)
{
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  const uint64_t saved_cluster_version = GET_MIN_CLUSTER_VERSION();
  const uint64_t saved_data_version = ATOMIC_LOAD(&cluster_version.data_version_);
  const uint64_t saved_mock_data_version = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  ObArenaAllocator allocator("GCHistByteTest");
  ObArray<ObSharedGCTask> tasks;
  ObSharedGCMacroIdList macro_ids;
  ObString value;
  ObSqlString sql;
  const int64_t sql_limit = 2 * 1024 * 1024;
  int64_t task_idx = 0;
  int64_t previous_idx = 0;
  int64_t row_count = 0;
  int64_t total_rows = 0;
  int64_t batch_count = 0;
  DEFER({
    cluster_version.update_cluster_version(saved_cluster_version);
    cluster_version.update_data_version(saved_data_version);
    ODV_MGR.set_mock_data_version(saved_mock_data_version);
  });
  cluster_version.update_data_version(0);
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_0);
  ASSERT_EQ(OB_SUCCESS, build_history_tasks(allocator, 1817, 8, 256 * 1024, tasks));
  ASSERT_EQ(OB_SUCCESS, build_mark_sweep_stat_value(allocator, 1, 1, macro_ids, value));
  for (int64_t i = 0; i < tasks.count(); ++i) {
    tasks.at(i).extra_info_.assign_ptr(tasks.at(i).value_.ptr(), 16 * 1024);
    tasks.at(i).trace_id_.assign_ptr(tasks.at(i).value_.ptr(), 1024);
    tasks.at(i).server_id_ = INT64_MAX;
    if (1 == i % 2) {
      tasks.at(i).key_.type_ = ObSharedGCTaskType::MARK_SWEEP_GC;
      tasks.at(i).value_ = value;
    }
    ASSERT_TRUE(tasks.at(i).is_valid());
  }
  while (task_idx < tasks.count()) {
    int64_t expected_rows = 0;
    previous_idx = task_idx;
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
        tasks, task_idx, sql, row_count));
    ASSERT_GT(task_idx, previous_idx);
    ASSERT_LE(sql.length(), sql_limit);
    for (int64_t i = previous_idx; i < task_idx; ++i) {
      expected_rows += 1 == i % 2 ? 3 : 1;
    }
    ASSERT_EQ(expected_rows, row_count);
    ASSERT_LE(row_count, 100);
    total_rows += row_count;
    ++batch_count;
  }
  ASSERT_GT(batch_count, 1);
  ASSERT_EQ(16, total_rows);
  task_idx = -1;
  ASSERT_EQ(OB_SUCCESS, sql.assign("old sql"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(-1, task_idx);
  ASSERT_TRUE(sql.empty());
  ASSERT_EQ(0, row_count);
}

TEST_F(TestSharedGCTaskTableOperator, history_batch_sql_exact_byte_limit)
{
  ObArenaAllocator allocator("GCHistEdgeTest");
  ObArray<ObSharedGCTask> tasks;
  ObSqlString sql;
  const int64_t sql_limit = 2 * 1024 * 1024;
  const int64_t initial_value_size = 200 * 1024;
  int64_t task_idx = 0;
  int64_t row_count = 0;
  int64_t boundary_value_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_history_tasks(allocator, 1818, 4, 256 * 1024, tasks));
  tasks.at(3).value_.assign_ptr(tasks.at(3).value_.ptr(), initial_value_size);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(4, task_idx);
  ASSERT_EQ(4, row_count);
  ASSERT_LT(sql.length(), sql_limit);
  // Increasing a binary value by one byte adds exactly two hexadecimal SQL bytes.
  // Start from the complete SQL so its header and every row separator are included.
  boundary_value_size = initial_value_size + (sql_limit - sql.length()) / 2;
  ASSERT_LT(boundary_value_size, ObSharedGCTask::MAX_VALUE_LENGTH);
  tasks.at(3).value_.assign_ptr(tasks.at(3).value_.ptr(), boundary_value_size);
  task_idx = 0;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(4, task_idx);
  ASSERT_EQ(4, row_count);
  ASSERT_GE(sql.length(), sql_limit - 1);
  ASSERT_LE(sql.length(), sql_limit);
  tasks.at(3).value_.assign_ptr(tasks.at(3).value_.ptr(), boundary_value_size + 1);
  task_idx = 0;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(3, task_idx);
  ASSERT_EQ(3, row_count);
  ASSERT_LE(sql.length(), sql_limit);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(4, task_idx);
  ASSERT_EQ(1, row_count);
  ASSERT_LE(sql.length(), sql_limit);
}

TEST_F(TestSharedGCTaskTableOperator, history_batch_sql_reuse)
{
  ObArenaAllocator allocator("GCHistReuseTest");
  ObArray<ObSharedGCTask> tasks;
  ObSqlString sql;
  const int64_t sql_limit = ObSharedGCTaskTableOperator::MAX_HISTORY_BATCH_SQL_LENGTH;
  int64_t task_idx = 0;
  int64_t row_count = 0;
  int64_t first_capacity = 0;
  int64_t first_length = 0;
  ASSERT_EQ(OB_SUCCESS, build_history_tasks(allocator, 1820, 4, 256 * 1024, tasks));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(3, task_idx);
  ASSERT_EQ(3, row_count);
  ASSERT_LE(sql.length(), sql_limit);
  first_capacity = sql.capacity();
  first_length = sql.length();
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(4, task_idx);
  ASSERT_EQ(1, row_count);
  // A smaller batch retains the previous SQL buffer capacity.
  ASSERT_EQ(first_capacity, sql.capacity());
  ASSERT_LT(sql.length(), first_length);
  ASSERT_LE(sql.length(), sql_limit);
}

TEST_F(TestSharedGCTaskTableOperator, history_batch_sql_optional_columns)
{
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  const uint64_t saved_cluster_version = GET_MIN_CLUSTER_VERSION();
  const uint64_t saved_data_version = ATOMIC_LOAD(&cluster_version.data_version_);
  const uint64_t saved_mock_data_version = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  ObArenaAllocator allocator("GCHistColsTest");
  ObArenaAllocator read_allocator("GCHistColsRead");
  ObArray<ObSharedGCTask> tasks;
  ObSharedGCMacroIdList macro_ids;
  ObSharedGCTask expected;
  ObSharedGCTask actual;
  ObString value;
  ObSqlString sql;
  const int64_t round_id = 1819;
  const int64_t task_count = 16;
  int64_t task_idx = 0;
  int64_t row_count = 0;
  int64_t affected_rows = 0;
  DEFER({
    cluster_version.update_cluster_version(saved_cluster_version);
    cluster_version.update_data_version(saved_data_version);
    ODV_MGR.set_mock_data_version(saved_mock_data_version);
  });
  cluster_version.update_data_version(0);
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_0);
  ASSERT_EQ(OB_SUCCESS, build_history_tasks(allocator, round_id, task_count, 1, tasks));
  ASSERT_EQ(OB_SUCCESS, build_mark_sweep_stat_value(allocator, 1, 1, macro_ids, value));
  ASSERT_EQ(OB_SUCCESS, delete_history_by_round(TENANT_ID, round_id, sql_proxy()));
  DEFER((void)delete_history_by_round(TENANT_ID, round_id, sql_proxy()));
  for (int64_t i = 0; i < tasks.count(); ++i) {
    const int64_t field_mask = i % 8;
    if (i >= 8) {
      tasks.at(i).key_.type_ = ObSharedGCTaskType::MARK_SWEEP_GC;
      tasks.at(i).value_ = value;
    }
    if (0 != (field_mask & 1)) {
      tasks.at(i).extra_info_ = ObString::make_string("extra 'quoted' value");
    }
    if (0 != (field_mask & 2)) {
      tasks.at(i).server_id_ = 123456789;
    }
    if (0 != (field_mask & 4)) {
      tasks.at(i).trace_id_ = ObString::make_string("trace 'quoted' value");
    }
  }
  // All rows must match the fixed eight-column header, including rows with NULL
  // optional fields and derived statistics with inherited fields.
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(task_count, task_idx);
  ASSERT_EQ(32, row_count);
  ASSERT_EQ(OB_SUCCESS, sql_proxy().write(TENANT_ID, sql.ptr(), affected_rows));
  ASSERT_EQ(row_count, affected_rows);
  for (int64_t i = 0; i < tasks.count(); ++i) {
    const int64_t group_rows = i >= 8 ? 3 : 1;
    for (int64_t j = 0; j < group_rows; ++j) {
      expected = tasks.at(i);
      if (1 == j) {
        expected.key_.type_ = ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT;
        expected.value_ = ObString::make_string("1");
      } else if (2 == j) {
        expected.key_.type_ = ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT;
        expected.value_ = ObString::make_string("2");
      }
      read_allocator.reuse();
      ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
          expected.key_, read_allocator, sql_proxy(), actual));
      ASSERT_EQ(expected.key_.tenant_id_, actual.key_.tenant_id_);
      ASSERT_EQ(expected.key_.type_, actual.key_.type_);
      ASSERT_EQ(expected.key_.round_id_, actual.key_.round_id_);
      ASSERT_EQ(expected.key_.task_id_, actual.key_.task_id_);
      ASSERT_EQ(expected.value_, actual.value_);
      ASSERT_EQ(expected.extra_info_, actual.extra_info_);
      ASSERT_EQ(expected.server_id_, actual.server_id_);
      ASSERT_EQ(expected.trace_id_, actual.trace_id_);
    }
  }
}

TEST_F(TestSharedGCTaskTableOperator, history_batch_sql_maximum_mark_sweep_group)
{
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  const uint64_t saved_cluster_version = GET_MIN_CLUSTER_VERSION();
  const uint64_t saved_data_version = ATOMIC_LOAD(&cluster_version.data_version_);
  const uint64_t saved_mock_data_version = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  ObArenaAllocator allocator("GCHistMaxTest");
  ObArray<ObSharedGCTask> tasks;
  ObSharedGCMacroIdList macro_ids;
  ObSharedGCTask task;
  blocksstable::MacroBlockId candidate;
  ObString value;
  ObSqlString sql;
  char *extra_info = nullptr;
  char *trace_id = nullptr;
  int64_t serialize_size = 0;
  int64_t next_serialize_size = 0;
  int64_t task_idx = 0;
  int64_t row_count = 0;
  bool value_limit_reached = false;
  DEFER({
    cluster_version.update_cluster_version(saved_cluster_version);
    cluster_version.update_data_version(saved_data_version);
    ODV_MGR.set_mock_data_version(saved_mock_data_version);
  });
  cluster_version.update_data_version(0);
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_0);
  ASSERT_EQ(OB_SUCCESS, build_mark_sweep_stat_value(allocator, 1, 1, macro_ids, value));
  serialize_size = macro_ids.get_serialize_size();
  candidate = macro_ids.at(0);
  while (!value_limit_reached) {
    candidate.set_second_id(5000 + macro_ids.count());
    candidate.set_seq_id(300 + macro_ids.count());
    ASSERT_TRUE(candidate.is_valid());
    ASSERT_EQ(OB_SUCCESS, macro_ids.get_serialize_size_after_add(
        candidate, serialize_size, next_serialize_size));
    if (next_serialize_size > ObSharedGCTask::MAX_VALUE_LENGTH) {
      value_limit_reached = true;
    } else {
      ASSERT_EQ(OB_SUCCESS, macro_ids.add(candidate));
      serialize_size = next_serialize_size;
    }
  }
  ASSERT_GT(macro_ids.count(), 2);
  ASSERT_LE(serialize_size, ObSharedGCTask::MAX_VALUE_LENGTH);
  ASSERT_GT(next_serialize_size, ObSharedGCTask::MAX_VALUE_LENGTH);
  ASSERT_EQ(OB_SUCCESS, macro_ids.to_value(allocator, value));
  ASSERT_EQ(serialize_size, value.length());
  extra_info = static_cast<char *>(allocator.alloc(ObSharedGCTask::MAX_EXTRA_INFO_LENGTH));
  trace_id = static_cast<char *>(allocator.alloc(ObSharedGCTask::MAX_TRACE_ID_LENGTH));
  ASSERT_NE(nullptr, extra_info);
  ASSERT_NE(nullptr, trace_id);
  MEMSET(extra_info, 'E', ObSharedGCTask::MAX_EXTRA_INFO_LENGTH);
  MEMSET(trace_id, 'T', ObSharedGCTask::MAX_TRACE_ID_LENGTH);
  task.key_ = ObSharedGCTaskKey(
      TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC, INT64_MAX, INT64_MAX);
  task.value_ = value;
  task.extra_info_.assign_ptr(extra_info, ObSharedGCTask::MAX_EXTRA_INFO_LENGTH);
  task.trace_id_.assign_ptr(trace_id, ObSharedGCTask::MAX_TRACE_ID_LENGTH);
  task.server_id_ = INT64_MAX;
  ASSERT_TRUE(task.is_valid());
  ASSERT_EQ(OB_SUCCESS, tasks.push_back(task));
  // This payload cannot accept one more valid macro id. Its complete three-row
  // group must still fit after SQL encoding, including maximum optional fields.
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(1, task_idx);
  ASSERT_EQ(3, row_count);
  LOG_INFO("maximum mark sweep history group", "macro_id_count", macro_ids.count(),
      "value_length", value.length(), "sql_length", sql.length(), K(row_count));
  ASSERT_LE(row_count, ObSharedGCTaskTableOperator::MAX_HISTORY_BATCH_ROW_COUNT);
  ASSERT_LE(sql.length(), ObSharedGCTaskTableOperator::MAX_HISTORY_BATCH_SQL_LENGTH);
}

TEST_F(TestSharedGCTaskTableOperator, history_batch_second_write_failure)
{
  ObArenaAllocator allocator("GCHistFailTest");
  ObArray<ObSharedGCTask> tasks;
  ObHistoryBatchSQLClient client(2/*fail_write_idx*/);
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, build_history_tasks(allocator, 1814, 10, 256 * 1024, tasks));
  ASSERT_EQ(OB_TIMEOUT, ObSharedGCTaskTableOperator::batch_insert_histories_(
      TENANT_ID, tasks, client, affected_rows));
  ASSERT_EQ(2, client.get_write_count());
  ASSERT_LE(client.get_max_sql_length(), 2 * 1024 * 1024);
}

TEST_F(TestSharedGCTaskTableOperator, batch_delete_histories_sql_memory)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("GCHistSQLTest");
  ObArray<ObSharedGCTask> tasks;
  ObArray<ObSharedGCTask> warmup_tasks;
  ObSqlString sql;
  const int64_t round_id = 1815;
  const int64_t value_size = 256 * 1024;
  int64_t public_peak_memory = 0;
  int64_t peak_memory = 0;
  int64_t retained_memory = 0;
  int64_t sql_peak_memory = 0;
  int64_t sql_retained_memory = 0;
  int64_t write_count = 0;
  int64_t row_count = 0;
  int64_t value_length = 0;
  ASSERT_EQ(OB_SUCCESS, build_history_tasks(allocator, round_id, 100, value_size, tasks));
  ASSERT_EQ(OB_SUCCESS, delete_history_by_round(TENANT_ID, round_id, sql_proxy()));
  DEFER({
    for (int64_t i = 0; i < tasks.count(); ++i) {
      (void)ObSharedGCTaskTableOperator::delete_task(tasks.at(i).key_, sql_proxy());
    }
    (void)delete_history_by_round(TENANT_ID, round_id, sql_proxy());
  });
  for (int64_t i = 0; i < tasks.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(tasks.at(i), sql_proxy()));
  }
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, warmup_tasks.push_back(tasks.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_delete_tasks_and_insert_histories(
      TENANT_ID, warmup_tasks));
  {
    // Record allocation events visible to the calling context. Execution paths
    // and caches may vary, so SQL execution memory measurements are telemetry only.
    observer::ObProcessMallocCallback callback(0, public_peak_memory);
    lib::ObMallocCallbackGuard guard(callback);
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_delete_tasks_and_insert_histories(
        TENANT_ID, tasks));
  }
  ASSERT_EQ(OB_SUCCESS, get_history_count(round_id, sql_proxy(), row_count, value_length));
  ASSERT_EQ(100, row_count);
  ASSERT_EQ(100 * value_size, value_length);
  ASSERT_EQ(OB_SUCCESS, delete_history_by_round(TENANT_ID, round_id, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, insert_histories_with_memory(tasks, peak_memory, retained_memory,
      sql_peak_memory, sql_retained_memory, write_count));
  // Each write starts from zero. Callbacks report calling-context peaks and net
  // allocation deltas, not process RSS or complete transaction memory.
#ifndef OB_USE_ASAN
  LOG_INFO("history batch inner sql memory observation", K(public_peak_memory), K(peak_memory),
      K(retained_memory), K(sql_peak_memory), K(sql_retained_memory), K(write_count));
#else
  LOG_INFO("history batch inner sql memory callback unsupported under ASAN", K(write_count));
#endif
  ASSERT_GE(write_count, 34);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_delete_tasks_and_insert_histories(
      TENANT_ID, tasks));
  ASSERT_EQ(OB_SUCCESS, get_history_count(round_id, sql_proxy(), row_count, value_length));
  ASSERT_EQ(100, row_count);
  ASSERT_EQ(100 * value_size, value_length);
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT COUNT(*) AS row_count FROM oceanbase.%s "
      "WHERE tenant_id = %lu AND round_id = %ld",
      OB_ALL_SHARED_GC_TASK_TNAME, TENANT_ID, round_id));
  HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy().read(res, TENANT_ID, sql.ptr()));
    ASSERT_NE(nullptr, result = res.get_result());
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("row_count", row_count));
    ASSERT_EQ(0, row_count);
  }
}

TEST_F(TestSharedGCTaskTableOperator, batch_delete_histories_later_build_failure_rollback)
{
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  const uint64_t saved_cluster_version = GET_MIN_CLUSTER_VERSION();
  const uint64_t saved_data_version = ATOMIC_LOAD(&cluster_version.data_version_);
  const uint64_t saved_mock_data_version = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  ObArenaAllocator allocator("GCHistRollTest");
  ObArenaAllocator read_allocator("GCHistReadTest");
  ObArray<ObSharedGCTask> tasks;
  ObSharedGCTask task;
  ObSharedGCMacroIdList macro_ids;
  ObString valid_value;
  ObSqlString sql;
  const int64_t round_id = 1816;
  int64_t task_idx = 0;
  int64_t row_count = 0;
  int64_t value_length = 0;
  DEFER({
    cluster_version.update_cluster_version(saved_cluster_version);
    cluster_version.update_data_version(saved_data_version);
    ODV_MGR.set_mock_data_version(saved_mock_data_version);
    for (int64_t i = 0; i < tasks.count(); ++i) {
      (void)ObSharedGCTaskTableOperator::delete_task(tasks.at(i).key_, sql_proxy());
    }
    (void)delete_history_by_round(TENANT_ID, round_id, sql_proxy());
  });
  cluster_version.update_data_version(0);
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_0);
  ASSERT_EQ(OB_SUCCESS, build_history_tasks(allocator, round_id, 5, 256 * 1024, tasks));
  tasks.at(4).key_.type_ = ObSharedGCTaskType::MARK_SWEEP_GC;
  ASSERT_TRUE(tasks.at(4).is_valid());
  ASSERT_EQ(OB_SUCCESS, delete_history_by_round(TENANT_ID, round_id, sql_proxy()));
  for (int64_t i = 0; i < tasks.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(tasks.at(i), sql_proxy()));
  }
  // The fourth large valid task crosses the first batch boundary before the fifth
  // malformed task is inspected, so the public path must roll back a successful INSERT.
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count));
  ASSERT_EQ(3, task_idx);
  ASSERT_EQ(3, row_count);
  const int expected_error = ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
      tasks, task_idx, sql, row_count);
  ASSERT_NE(OB_SUCCESS, expected_error);
  ASSERT_EQ(3, task_idx);
  ASSERT_TRUE(sql.empty());
  ASSERT_EQ(0, row_count);
  ASSERT_EQ(expected_error, ObSharedGCTaskTableOperator::batch_delete_tasks_and_insert_histories(
      TENANT_ID, tasks));
  ASSERT_EQ(OB_SUCCESS, get_history_count(round_id, sql_proxy(), row_count, value_length));
  ASSERT_EQ(0, row_count);
  for (int64_t i = 0; i < tasks.count(); ++i) {
    read_allocator.reuse();
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_task(
        tasks.at(i).key_, read_allocator, sql_proxy(), task));
    ASSERT_EQ(tasks.at(i).value_, task.value_);
  }
  ASSERT_EQ(OB_SUCCESS, build_mark_sweep_stat_value(allocator, 1, 1, macro_ids, valid_value));
  tasks.at(4).value_ = valid_value;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_delete_tasks_and_insert_histories(
      TENANT_ID, tasks));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_delete_tasks_and_insert_histories(
      TENANT_ID, tasks));
  ASSERT_EQ(OB_SUCCESS, get_history_count(round_id, sql_proxy(), row_count, value_length));
  ASSERT_EQ(7, row_count);
  for (int64_t i = 0; i < tasks.count(); ++i) {
    read_allocator.reuse();
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObSharedGCTaskTableOperator::get_task(
        tasks.at(i).key_, read_allocator, sql_proxy(), task));
  }
}

// =====================================================================
// Cross-table integration: move row from task to history in a single trans.
// This is the canonical pattern the GC algorithm uses after a task is
// successfully GC'd: insert_history + delete_task inside one ObMySQLTransaction,
// then commit. After commit, the row exists in history, not in task.
// =====================================================================
TEST_F(TestSharedGCTaskTableOperator, move_task_to_history_in_trans)
{
  ObArenaAllocator allocator;
  const int64_t round_id = 1900;
  const int64_t task_id = 1;

  ObSharedGCTask task; ObSharedGCFailInfo info;
  ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, round_id, task_id, task, info));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));

  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy(), TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_history(task, trans));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(task.key_, trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(true /*commit*/));

  // After commit: row is in history, not in task
  ObSharedGCTask history_row;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      task.key_, allocator, sql_proxy(), history_row));
  ASSERT_EQ(task.key_.task_id_, history_row.key_.task_id_);

  {
    const ObSSGCTaskIdRange iter_range(0, INT64_MAX);
    ObSharedGCTaskTableRangeIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(TENANT_ID, task.key_.type_, round_id, iter_range, sql_proxy()));
    ObSharedGCTask t;
    ASSERT_EQ(OB_ITER_END, iter.get_next(allocator, t));
  }

  // Cleanup
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_history(task.key_, sql_proxy()));
}

// Rollback variant: on rollback neither insert_history nor delete_task should
// take effect.
TEST_F(TestSharedGCTaskTableOperator, move_task_to_history_rollback)
{
  ObArenaAllocator allocator;
  const int64_t round_id = 2000;
  const int64_t task_id = 1;

  ObSharedGCTask task; ObSharedGCFailInfo info;
  ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, round_id, task_id, task, info));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));

  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy(), TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_history(task, trans));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(task.key_, trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(false /*rollback*/));

  // Task row still there, no history row
  {
    const ObSSGCTaskIdRange iter_range(0, INT64_MAX);
    ObSharedGCTaskTableRangeIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(TENANT_ID, task.key_.type_, round_id, iter_range, sql_proxy()));
    ObSharedGCTask t;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, t));
    ASSERT_EQ(round_id, t.key_.round_id_);
  }

  ObSharedGCTask absent;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            ObSharedGCTaskTableOperator::get_history(task.key_, allocator,
                                                     sql_proxy(), absent));

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(task.key_, sql_proxy()));
}

// =====================================================================
// Validation: invalid arguments across all public methods.
// =====================================================================
TEST_F(TestSharedGCTaskTableOperator, invalid_argument)
{
  ObArenaAllocator allocator;
  // Build a sane task once, then poke individual fields invalid.
  ObSharedGCTask base; ObSharedGCFailInfo info;
  ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, 2100, 1, base, info));

  // 1) Invalid task: tenant_id = OB_INVALID_TENANT_ID
  {
    ObSharedGCTask t = base;
    t.key_.tenant_id_ = OB_INVALID_TENANT_ID;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCTaskTableOperator::insert_task(t, sql_proxy()));
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCTaskTableOperator::insert_history(t, sql_proxy()));
  }
  // 2) Invalid task type
  {
    ObSharedGCTask t = base;
    t.key_.type_ = ObSharedGCTaskType::INVALID_TYPE;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCTaskTableOperator::insert_task(t, sql_proxy()));
  }
  // 3) Negative round_id
  {
    ObSharedGCTask t = base;
    t.key_.round_id_ = -1;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCTaskTableOperator::insert_task(t, sql_proxy()));
  }
  // 4) Empty FAIL_GC values are rejected; only DIAG round markers may omit the payload.
  {
    ObSharedGCTask t = base;
    t.value_.reset();
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCTaskTableOperator::insert_task(t, sql_proxy()));
  }
  // 5) batch_insert_tasks with empty vector
  {
    ObArray<ObSharedGCTask> empty;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCTaskTableOperator::batch_insert_tasks(TENANT_ID, empty, sql_proxy()));
  }
  // 6) iterator init with invalid arguments
  {
    const ObSSGCTaskIdRange any_range;
    ObSharedGCTaskTableRangeIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              iter.init(TENANT_ID, ObSharedGCTaskType::INVALID_TYPE,
                        0, any_range, sql_proxy()));
    ObSharedGCTaskTableRangeIterator iter2;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              iter2.init(OB_INVALID_TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC,
                         0, any_range, sql_proxy()));
  }
  // 7) update / delete with invalid key
  {
    ObSharedGCTaskKey bad_key;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCTaskTableOperator::update_extra_info(
                  bad_key, ObString::make_string("x"), sql_proxy()));
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
                  bad_key, 1, ObString::make_string("trc"), sql_proxy()));
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCTaskTableOperator::delete_task(bad_key, sql_proxy()));
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCTaskTableOperator::delete_history(bad_key, sql_proxy()));
    ObSharedGCTask out;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCTaskTableOperator::get_history(bad_key, allocator,
                                                       sql_proxy(), out));
  }
  // 8) extra_info / trace_id length over limit
  {
    ObSharedGCTaskKey key(TENANT_ID, ObSharedGCTaskType::FAIL_GC, 2100, 1);
    char *huge = static_cast<char *>(allocator.alloc(ObSharedGCTask::MAX_EXTRA_INFO_LENGTH + 16));
    ASSERT_NE(nullptr, huge);
    memset(huge, 'A', ObSharedGCTask::MAX_EXTRA_INFO_LENGTH + 16);
    ObString too_long(ObSharedGCTask::MAX_EXTRA_INFO_LENGTH + 16, huge);
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCTaskTableOperator::update_extra_info(key, too_long, sql_proxy()));

    char *huge_trc = static_cast<char *>(allocator.alloc(ObSharedGCTask::MAX_TRACE_ID_LENGTH + 4));
    ASSERT_NE(nullptr, huge_trc);
    memset(huge_trc, 'B', ObSharedGCTask::MAX_TRACE_ID_LENGTH + 4);
    ObString too_long_trc(ObSharedGCTask::MAX_TRACE_ID_LENGTH + 4, huge_trc);
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCTaskTableOperator::update_server_id_and_trace_id(key, 1, too_long_trc, sql_proxy()));
  }
}

// =====================================================================
// 崩溃接管容忍。
//
// 新 Executor 只能接管 owner 已超时的行。不同 live owner 和 active 行不存在
// 不能当作认领成功，否则调用方会在没有 owner 权限时重复执行 Step C。
// =====================================================================

TEST_F(TestSharedGCTaskTableOperator, claim_by_live_server_rejected)
{
  ObArenaAllocator allocator;
  const int64_t round_id = 2200;
  const int64_t task_id = 1;

  ObSharedGCTask task;
  ObSharedGCFailInfo info;
  ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, round_id, task_id, task, info));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));

  const ObString first_trace  = ObString::make_string("trc-first");
  const ObString second_trace = ObString::make_string("trc-second");

  // 第一个 server 认领行。
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
      task.key_, 1001 /*server_id*/, first_trace, sql_proxy()));

  // 另一个 server 不能抢占 live owner。
  ASSERT_EQ(OB_ENTRY_EXIST, ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
      task.key_, 2002 /*server_id*/, second_trace, sql_proxy()));

  const ObSSGCTaskIdRange iter_range(0, INT64_MAX);
  ObSharedGCTaskTableRangeIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(TENANT_ID, task.key_.type_, round_id, iter_range, sql_proxy()));
  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, row));
  ASSERT_EQ(1001, row.server_id_);
  ASSERT_EQ(first_trace, row.trace_id_);

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(task.key_, sql_proxy()));
}

TEST_F(TestSharedGCTaskTableOperator, claim_after_moved_to_history_rejected)
{
  ObArenaAllocator allocator;
  const int64_t round_id = 2210;
  const int64_t task_id = 1;

  ObSharedGCTask task;
  ObSharedGCFailInfo info;
  ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, round_id, task_id, task, info));

  // 并发 Executor 已完成此 key：history 存在，active 行不存在。
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_history(task, sql_proxy()));

  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
      task.key_, 3333, ObString::make_string("trc-late"), sql_proxy()));

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_history(task.key_, sql_proxy()));
}

// 端到端接管：新 Executor 接管崩溃 peer 认领过的行，走完整的
// delete_task_and_insert_history 流程；无论是残留认领还是 history 行重复
// 都不会报错。覆盖 ObExecuteBaseTask::process_row_ 使用的组合路径。
TEST_F(TestSharedGCTaskTableOperator, takeover_after_crash_end_to_end)
{
  ObArenaAllocator allocator;
  const int64_t round_id = 2240;
  const int64_t task_id = 1;

  ObSharedGCTask task;
  ObSharedGCFailInfo info;
  ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, round_id, task_id, task, info));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));

  // Peer A 认领后不再推进。回拨 gmt_modified，模拟 owner 已超时，
  // 使 Peer B 可以接管。
  const ObString trace_a = ObString::make_string("trc-crashed-peer");
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
      task.key_, 1 /*peer_a*/, trace_a, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, backdate_active_task_modified(
      task.key_, 24L * 60 * 60 * 1000 * 1000 /*age_us*/, sql_proxy()));

  // Peer B 只能在旧 owner 超时后认领。
  const ObString trace_b = ObString::make_string("trc-taker");
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
      task.key_, 2 /*peer_b*/, trace_b, sql_proxy()));

  // Peer B 挪 history：首次调用正常提交。
  ObSharedGCTask peer_b_task = task;
  peer_b_task.server_id_ = 2;
  peer_b_task.trace_id_ = trace_b;
  ObMySQLProxy &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task_and_insert_history(
      peer_b_task, proxy));

  // 此时行只在 history 中。
  ObSharedGCTask history_row;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      task.key_, allocator, sql_proxy(), history_row));
  ASSERT_EQ(task.key_.task_id_, history_row.key_.task_id_);

  // Peer A 假设苏醒后再重放同样的迁移：delete 空删、history INSERT 主键冲突，
  // 都被容忍。
  ObSharedGCTask peer_a_task = task;
  peer_a_task.server_id_ = 1;
  peer_a_task.trace_id_ = trace_a;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task_and_insert_history(
      peer_a_task, proxy));

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_history(task.key_, sql_proxy()));
}

// =====================================================================
// Helpers for delete_expired_history tests
// =====================================================================

int TestSharedGCTaskTableOperator::insert_history_backdated(
    const uint64_t tenant_id,
    const int64_t round_id,
    const int64_t task_id,
    const int64_t age_us,
    ObISQLClient &client)
{
  // build_fail_task hardcodes TENANT_ID (OB_SYS_TENANT_ID) internally;
  // callers must pass the same value or the backdate UPDATE will find no row.
  OB_ASSERT(tenant_id == TENANT_ID);
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("TestHisClean");
  ObSharedGCTask task;
  ObSharedGCFailInfo info;
  const uint64_t real_tenant_id = OB_SYS_TENANT_ID;
  if (OB_FAIL(build_fail_task(allocator, round_id, task_id, task, info))) {
    LOG_WARN("build_fail_task failed", K(ret));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::insert_history(task, client))) {
    LOG_WARN("insert_history failed", K(ret));
  } else {
    const int64_t old_ts_us = ObTimeUtility::current_time() - age_us;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt(
          "UPDATE %s SET gmt_create = usec_to_time(%ld) "
          "WHERE tenant_id = %lu AND type = %ld AND round_id = %ld AND task_id = %ld",
          OB_ALL_SHARED_GC_TASK_HISTORY_TNAME,
          old_ts_us,
          tenant_id,
          static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC),
          round_id,
          task_id))) {
      LOG_WARN("assign_fmt failed", K(ret));
    } else if (OB_FAIL(client.write(real_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("UPDATE gmt_create failed", K(ret), K(sql));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected_rows for backdate UPDATE", K(ret), K(affected_rows));
    }
  }
  return ret;
}

int TestSharedGCTaskTableOperator::backdate_active_task_modified(
    const ObSharedGCTaskKey &key,
    const int64_t age_us,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  const uint64_t real_tenant_id = OB_SYS_TENANT_ID;
  const int64_t old_ts_us = ObTimeUtility::current_time() - age_us;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.assign_fmt(
        "UPDATE %s SET gmt_modified = usec_to_time(%ld) "
        "WHERE tenant_id = %lu AND type = %ld AND round_id = %ld AND task_id = %ld",
        OB_ALL_SHARED_GC_TASK_TNAME,
        old_ts_us,
        key.tenant_id_,
        static_cast<int64_t>(key.type_),
        key.round_id_,
        key.task_id_))) {
    LOG_WARN("assign_fmt failed", K(ret));
  } else if (OB_FAIL(client.write(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("UPDATE gmt_modified failed", K(ret), K(sql));
  } else if (OB_UNLIKELY(1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected_rows for gmt_modified backdate", K(ret), K(affected_rows), K(key));
  }
  return ret;
}

int TestSharedGCTaskTableOperator::get_active_task_modified_us(
    const ObSharedGCTaskKey &key,
    ObISQLClient &client,
    int64_t &modified_us)
{
  int ret = OB_SUCCESS;
  const uint64_t real_tenant_id = OB_SYS_TENANT_ID;
  ObSqlString sql;
  modified_us = 0;
  if (OB_FAIL(sql.assign_fmt(
        "SELECT TIME_TO_USEC(gmt_modified) AS modified_us FROM %s "
        "WHERE tenant_id = %lu AND type = %ld AND round_id = %ld AND task_id = %ld",
        OB_ALL_SHARED_GC_TASK_TNAME,
        key.tenant_id_,
        static_cast<int64_t>(key.type_),
        key.round_id_,
        key.task_id_))) {
    LOG_WARN("assign_fmt failed", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(client.read(res, real_tenant_id, sql.ptr()))) {
        LOG_WARN("read gmt_modified failed", K(ret), K(sql));
      } else {
        sqlclient::ObMySQLResult *result = res.get_result();
        if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", K(ret));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("row not found", K(ret), K(key));
        } else if (OB_FAIL(result->get_int("modified_us", modified_us))) {
          LOG_WARN("failed to get modified_us", K(ret), K(key));
        }
      }
    }
  }
  return ret;
}

int TestSharedGCTaskTableOperator::delete_history_by_round(
    const uint64_t tenant_id,
    const int64_t round_id,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  const uint64_t real_tenant_id = OB_SYS_TENANT_ID;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.assign_fmt(
               "DELETE FROM %s WHERE tenant_id = %lu AND round_id = %ld",
               OB_ALL_SHARED_GC_TASK_HISTORY_TNAME,
               tenant_id, round_id))) {
    LOG_WARN("assign_fmt failed", K(ret));
  } else if (OB_FAIL(client.write(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("delete by round failed", K(ret), K(sql));
  }
  return ret;
}

// =====================================================================
// delete_expired_history tests
// =====================================================================

// No rows match the expire filter → OB_SUCCESS with affected_rows=0.
// Use epoch-time as expire_ts so no real row could ever qualify.
TEST_F(TestSharedGCTaskTableOperator, delete_expired_history_empty_table)
{
  int64_t affected_rows = -1;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_delete_expired(
      TENANT_ID, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME,
      1 /*epoch; no row is ever this old*/, 1000,
      sql_proxy(), affected_rows));
  ASSERT_EQ(0, affected_rows);
}

// Insert 3 rows older than retention + 2 rows newer than retention.
// After one cleanup call (limit >> total rows), the 3 old rows are gone
// and the 2 new rows survive.
TEST_F(TestSharedGCTaskTableOperator, delete_expired_history_retention_basic)
{
  const int64_t round_id   = 3000;
  const int64_t OLD_AGE_US = 10LL * 86400LL * 1000000LL;  // 10d → older than 7d retention
  const int64_t NEW_AGE_US =  1LL * 86400LL * 1000000LL;  //  1d → newer than 7d retention
  const int64_t RETENTION  =  7LL * 86400LL * 1000000LL;  //  7d

  // Pre-test cleanup in case of leftover rows from a prior crashed run.
  ASSERT_EQ(OB_SUCCESS, delete_history_by_round(TENANT_ID, round_id, sql_proxy()));

  for (int64_t tid = 1; tid <= 3; ++tid) {
    ASSERT_EQ(OB_SUCCESS,
              insert_history_backdated(TENANT_ID, round_id, tid, OLD_AGE_US, sql_proxy()));
  }
  for (int64_t tid = 4; tid <= 5; ++tid) {
    ASSERT_EQ(OB_SUCCESS,
              insert_history_backdated(TENANT_ID, round_id, tid, NEW_AGE_US, sql_proxy()));
  }

  const int64_t expire_ts_us = ObTimeUtility::current_time() - RETENTION;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_delete_expired(
      TENANT_ID, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME,
      expire_ts_us, 1000, sql_proxy(), affected_rows));
  ASSERT_EQ(3, affected_rows);

  // Old rows are gone.
  ObArenaAllocator allocator;
  ObSharedGCTask task;
  for (int64_t tid = 1; tid <= 3; ++tid) {
    ObSharedGCTaskKey key(TENANT_ID, ObSharedGCTaskType::FAIL_GC, round_id, tid);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST,
              ObSharedGCTaskTableOperator::get_history(key, allocator, sql_proxy(), task));
  }
  // New rows are still there.
  for (int64_t tid = 4; tid <= 5; ++tid) {
    ObSharedGCTaskKey key(TENANT_ID, ObSharedGCTaskType::FAIL_GC, round_id, tid);
    ASSERT_EQ(OB_SUCCESS,
              ObSharedGCTaskTableOperator::get_history(key, allocator, sql_proxy(), task));
  }

  ASSERT_EQ(OB_SUCCESS, delete_history_by_round(TENANT_ID, round_id, sql_proxy()));
}

// All three aggregate readers (get_min_round_id, get_round_id_range,
// get_task_id_range) use `GROUP BY type` so an empty matching set produces
// 0 rows (next() -> OB_ITER_END -> is_empty=true) instead of 1 row of NULLs.
// This test verifies they report is_empty=true rather than failing or returning
// stale data.
TEST_F(TestSharedGCTaskTableOperator, aggregate_readers_on_empty_set)
{
  // Drain the table so the MARK_SWEEP_GC bucket is provably empty. Other
  // tests in this binary may have left rows behind.
  bool dummy_stop = false;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_delete(TENANT_ID, dummy_stop));

  // get_min_round_id: WHERE tenant_id=? AND type=MARK_SWEEP_GC AND round_id<=?
  // allow_gc_round_id is required to be >= 0 by the operator's arg check, so
  // use INT64_MAX; with the bucket empty the predicate matches nothing.
  {
    bool is_empty = false;
    int64_t min_round_id = 0;
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_min_round_id(
        TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC,
        INT64_MAX /*allow_gc_round_id*/, sql_proxy(),
        is_empty, min_round_id));
    ASSERT_TRUE(is_empty);
  }

  // get_task_id_range: WHERE tenant_id=? AND type=? AND round_id=?
  // Pick a round_id high enough that no other test could have inserted there
  // (belt-and-suspenders on top of the batch_delete above).
  constexpr int64_t EMPTY_ROUND_ID = 1'000'000'000LL;
  for (const ObSharedGCTaskType type :
       {ObSharedGCTaskType::MARK_SWEEP_GC, ObSharedGCTaskType::FAIL_GC}) {
    bool is_empty = false;
    ObSSGCTaskIdRange round_range;
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_task_id_range(
        TENANT_ID, type, EMPTY_ROUND_ID, sql_proxy(), is_empty, round_range));
    ASSERT_TRUE(is_empty);
  }
}

// get_round_id_range happy-path + empty-set coverage.
//   - Empty: clear the table via batch_delete, then verify both type buckets
//     report is_empty=true. With GROUP BY type, the empty set yields 0 rows
//     (OB_ITER_END), not 1 row of NULLs.
//   - Non-empty: insert rows with known round_ids for both types and verify
//     min/max match per-type, independent of the other type's rows. This also
//     exercises the assumption that GROUP BY columns don't need to be in the
//     SELECT list: the SQL is `SELECT MIN(round_id), MAX(round_id) ...
//     GROUP BY type`, and we still read min/max by alias without touching
//     the type column.
TEST_F(TestSharedGCTaskTableOperator, get_round_id_range_basic)
{
  ObArenaAllocator allocator;
  bool dummy_stop = false;
  bool is_empty = true;
  int64_t min_round_id = -1;
  int64_t max_round_id = -1;

  // Pre-clean: drain leftover rows from any prior test/crash. batch_delete
  // sweeps both MARK_SWEEP_GC and FAIL_GC buckets.
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_delete(TENANT_ID, dummy_stop));

  // Empty bucket: aggregate returns NULL, must surface as is_empty=true.
  for (const ObSharedGCTaskType type :
       {ObSharedGCTaskType::MARK_SWEEP_GC, ObSharedGCTaskType::FAIL_GC}) {
    is_empty = false;
    min_round_id = -1;
    max_round_id = -1;
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_round_id_range(
        TENANT_ID, type, sql_proxy(), is_empty, min_round_id, max_round_id));
    ASSERT_TRUE(is_empty) << "type=" << static_cast<int>(type);
  }

  // Insert FAIL_GC rows at round_ids {5000, 5100, 5200} - one task each.
  for (const int64_t rid : {5000LL, 5100LL, 5200LL}) {
    ObSharedGCTask task; ObSharedGCFailInfo info;
    ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, rid, 1 /*task_id*/, task, info));
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));
  }
  // Insert one MARK_SWEEP_GC row at a distinct round_id so we can confirm
  // get_round_id_range filters by type.
  {
    ObSharedGCMacroIdList list;
    ObString value;
    ASSERT_EQ(OB_SUCCESS, build_mark_sweep_value(allocator, 1 /*id_count*/, list, value));
    ObSharedGCTask task;
    task.key_ = ObSharedGCTaskKey(TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC,
                                  9999 /*round_id*/, 1 /*task_id*/);
    task.value_ = value;
    task.extra_info_ = ObString::make_string("ms");
    task.server_id_ = ObSharedGCTask::INVALID_SERVER_ID;
    task.trace_id_.reset();
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));
  }

  // FAIL_GC: min=5000, max=5200.
  is_empty = true;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_round_id_range(
      TENANT_ID, ObSharedGCTaskType::FAIL_GC, sql_proxy(),
      is_empty, min_round_id, max_round_id));
  ASSERT_FALSE(is_empty);
  ASSERT_EQ(5000, min_round_id);
  ASSERT_EQ(5200, max_round_id);

  // MARK_SWEEP_GC: min=max=9999 (single row, distinct from FAIL_GC rows).
  is_empty = true;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_round_id_range(
      TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC, sql_proxy(),
      is_empty, min_round_id, max_round_id));
  ASSERT_FALSE(is_empty);
  ASSERT_EQ(9999, min_round_id);
  ASSERT_EQ(9999, max_round_id);

  // Cleanup so the next test starts clean.
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_delete(TENANT_ID, dummy_stop));
}

// Direct probe of the assumption behind the GROUP BY fix:
//   SELECT MIN(round_id), MAX(round_id) FROM t WHERE ... GROUP BY type
// must be accepted by OB even though the GROUP BY column `type` is not in
// the SELECT list (some engines reject this; OB / MySQL should not).
// We also verify that when WHERE pins `type` to a single value, the result
// is exactly one row carrying the per-type aggregate.
TEST_F(TestSharedGCTaskTableOperator, group_by_unselected_column_works)
{
  int ret =  OB_SUCCESS;
  ObArenaAllocator allocator;
  bool dummy_stop = false;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_delete(TENANT_ID, dummy_stop));

  // Insert 3 FAIL_GC rows + 1 MARK_SWEEP_GC row so the table has multiple types.
  for (const int64_t rid : {6000LL, 6001LL, 6002LL}) {
    ObSharedGCTask task; ObSharedGCFailInfo info;
    ASSERT_EQ(OB_SUCCESS, build_fail_task(allocator, rid, 1 /*task_id*/, task, info));
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));
  }
  {
    ObSharedGCMacroIdList list;
    ObString value;
    ASSERT_EQ(OB_SUCCESS, build_mark_sweep_value(allocator, 1, list, value));
    ObSharedGCTask task;
    task.key_ = ObSharedGCTaskKey(TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC,
                                  7000, 1);
    task.value_ = value;
    task.extra_info_ = ObString::make_string("ms");
    task.server_id_ = ObSharedGCTask::INVALID_SERVER_ID;
    task.trace_id_.reset();
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));
  }

  // Issue the exact SQL shape the operator builds: GROUP BY type without
  // including type in the SELECT list.
  const uint64_t real_tenant_id = TENANT_ID;  // sys tenant maps to itself
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT MIN(round_id) AS min_round_id, MAX(round_id) AS max_round_id "
      "FROM %s WHERE tenant_id = %lu AND type = %ld GROUP BY type",
      OB_ALL_SHARED_GC_TASK_TNAME,
      TENANT_ID,
      static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC)));

  HEAP_VAR(ObMySQLProxy::MySQLResult, result) {
    sqlclient::ObMySQLResult *r = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy().read(result, real_tenant_id, sql.ptr()));
    ASSERT_NE(nullptr, r = result.get_result());

    // First (and only) row: aggregates over the FAIL_GC bucket.
    ASSERT_EQ(OB_SUCCESS, r->next());
    int64_t min_rid = -1;
    int64_t max_rid = -1;
    ASSERT_EQ(OB_SUCCESS, r->get_int("min_round_id", min_rid));
    ASSERT_EQ(OB_SUCCESS, r->get_int("max_round_id", max_rid));
    ASSERT_EQ(6000, min_rid);
    ASSERT_EQ(6002, max_rid);

    // No second row (WHERE pinned type to FAIL_GC -> at most one group).
    ASSERT_EQ(OB_ITER_END, r->next());
  }

  // Empty-set sanity check on the same SQL shape: WHERE selects no rows ->
  // GROUP BY yields zero groups -> first next() returns OB_ITER_END.
  ObSqlString empty_sql;
  ASSERT_EQ(OB_SUCCESS, empty_sql.assign_fmt(
      "SELECT MIN(round_id) AS min_round_id, MAX(round_id) AS max_round_id "
      "FROM %s WHERE tenant_id = %lu AND type = %ld AND round_id = -1 "
      "GROUP BY type",
      OB_ALL_SHARED_GC_TASK_TNAME,
      TENANT_ID,
      static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC)));
  HEAP_VAR(ObMySQLProxy::MySQLResult, result2) {
    sqlclient::ObMySQLResult *r = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy().read(result2, real_tenant_id, empty_sql.ptr()));
    ASSERT_NE(nullptr, r = result2.get_result());
    ASSERT_EQ(OB_ITER_END, r->next());
  }

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::batch_delete(TENANT_ID, dummy_stop));
}

// Invalid arguments are rejected before any SQL is executed.
TEST_F(TestSharedGCTaskTableOperator, delete_expired_history_invalid_args)
{
  const int64_t valid_expire = ObTimeUtility::current_time() - 86400LL * 1000000LL;
  int64_t affected_rows = 0;

  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSharedGCTaskTableOperator::batch_delete_expired(
      OB_INVALID_TENANT_ID, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME,
      valid_expire, 1000, sql_proxy(), affected_rows));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSharedGCTaskTableOperator::batch_delete_expired(
      TENANT_ID, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME,
      0 /*expire_ts_us == 0*/, 1000, sql_proxy(), affected_rows));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSharedGCTaskTableOperator::batch_delete_expired(
      TENANT_ID, "invalid_table_name",
      valid_expire, 1000, sql_proxy(), affected_rows));
}

// =====================================================================
// FAIL_GC_MACRO_ID 历史行：generate_fail_gc_macro_id_history 每次调用会向
// __all_shared_gc_task_history 写入一个分片行，该行使用唯一递增的 task_id，
// round_id 固定为 0，extra_info 用于关联回源 FAIL_GC 任务。同一个源任务调用
// 两次会生成两个不同 task_id 的行，不会发生主键冲突，因此可以安全重试。
// =====================================================================
TEST_F(TestSharedGCTaskTableOperator, fail_gc_macro_id_history)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  const int64_t FAIL_GC_TASK_ID = 4242;

  // Build a macro id list and write it as a FAIL_GC_MACRO_ID history chunk.
  ObSharedGCMacroIdList list;
  ObString value;
  ASSERT_EQ(OB_SUCCESS, build_mark_sweep_value(allocator, 5 /*id_count*/, list, value));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskUtil::generate_fail_gc_macro_id_history(
      TENANT_ID, FAIL_GC_TASK_ID, list, sql_proxy()));
  // Second call (simulating a retry / a second chunk): must NOT collide on PK.
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskUtil::generate_fail_gc_macro_id_history(
      TENANT_ID, FAIL_GC_TASK_ID, list, sql_proxy()));

  // Read back: expect 2 rows, round_id==0, extra_info=="fail_gc_task_id=4242",
  // value round-trips to a 5-element list, task_ids distinct.
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT round_id, task_id, value FROM oceanbase.%s "
      "WHERE tenant_id = %lu AND type = %ld AND extra_info = 'fail_gc_task_id=%ld' "
      "ORDER BY task_id",
      OB_ALL_SHARED_GC_TASK_HISTORY_TNAME, TENANT_ID,
      static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC_MACRO_ID), FAIL_GC_TASK_ID));
  int64_t row_cnt = 0;
  int64_t prev_task_id = -1;
  HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *r = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy().read(res, TENANT_ID, sql.ptr()));
    ASSERT_NE(nullptr, r = res.get_result());
    while (OB_SUCC(r->next())) {
      int64_t round_id = -1;
      int64_t task_id = -1;
      ObString v;
      ASSERT_EQ(OB_SUCCESS, r->get_int("round_id", round_id));
      ASSERT_EQ(0, round_id);
      ASSERT_EQ(OB_SUCCESS, r->get_int("task_id", task_id));
      ASSERT_GT(task_id, prev_task_id);  // strictly increasing & distinct
      prev_task_id = task_id;
      ASSERT_EQ(OB_SUCCESS, r->get_varchar("value", v));
      ObArenaAllocator chunk_alloc;
      ObString deep_v;
      ASSERT_EQ(OB_SUCCESS, ob_write_string(chunk_alloc, v, deep_v));
      ObSharedGCMacroIdList decoded;
      ASSERT_EQ(OB_SUCCESS, decoded.from_value(deep_v));
      ASSERT_EQ(5, decoded.count());
      ++row_cnt;
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
  ASSERT_EQ(2, row_cnt);

  // Cleanup.
  ObSqlString del_sql;
  int64_t affected = 0;
  ASSERT_EQ(OB_SUCCESS, del_sql.assign_fmt(
      "DELETE FROM oceanbase.%s WHERE tenant_id = %lu AND type = %ld "
      "AND extra_info = 'fail_gc_task_id=%ld'",
      OB_ALL_SHARED_GC_TASK_HISTORY_TNAME, TENANT_ID,
      static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC_MACRO_ID), FAIL_GC_TASK_ID));
  ASSERT_EQ(OB_SUCCESS, sql_proxy().write(TENANT_ID, del_sql.ptr(), affected));
  ASSERT_EQ(2, affected);
}

TEST_F(TestSharedGCTaskTableOperator, macro_id_list_serialize_size_after_add)
{
  ObSharedGCMacroIdList list;
  blocksstable::MacroBlockId first_id(blocksstable::MacroBlockId::DEFAULT_FIRST_ID, 10001, 0, 0, 0);
  blocksstable::MacroBlockId second_id(blocksstable::MacroBlockId::DEFAULT_FIRST_ID, 10002, 0, 0, 0);
  int64_t cur_serialize_size = list.get_serialize_size();
  int64_t next_serialize_size = 0;

  ASSERT_EQ(OB_SUCCESS, list.get_serialize_size_after_add(
      first_id, cur_serialize_size, next_serialize_size));
  ASSERT_EQ(OB_SUCCESS, list.add(first_id));
  ASSERT_EQ(next_serialize_size, list.get_serialize_size());
  cur_serialize_size = next_serialize_size;

  ASSERT_EQ(OB_SUCCESS, list.get_serialize_size_after_add(
      second_id, cur_serialize_size, next_serialize_size));
  ASSERT_EQ(OB_SUCCESS, list.add(second_id));
  ASSERT_EQ(next_serialize_size, list.get_serialize_size());
}

// record_fail_gc_macro_ids_ must split an oversized macro-id array into multiple
// FAIL_GC_MACRO_ID history rows
TEST_F(TestSharedGCTaskTableOperator, fail_gc_macro_id_history_chunking)
{
  int ret = OB_SUCCESS;
  const int64_t FAIL_GC_TASK_ID = 525252;
  const int64_t EXTRA_OVERFLOW_ID_COUNT = 10;
  ObArray<blocksstable::MacroBlockId> macro_ids;
  ObSharedGCMacroIdList size_probe;
  int64_t id_count = 0;
  int64_t probe_serialize_size = size_probe.get_serialize_size();

  // Clean up rows possibly left by a previous interrupted run.
  {
    ObSqlString del_sql;
    int64_t affected = 0;
    ASSERT_EQ(OB_SUCCESS, del_sql.assign_fmt(
        "DELETE FROM oceanbase.%s WHERE tenant_id = %lu AND type = %ld "
        "AND extra_info = 'fail_gc_task_id=%ld'",
        OB_ALL_SHARED_GC_TASK_HISTORY_TNAME, TENANT_ID,
        static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC_MACRO_ID), FAIL_GC_TASK_ID));
    ASSERT_EQ(OB_SUCCESS, sql_proxy().write(TENANT_ID, del_sql.ptr(), affected));
  }

  while (probe_serialize_size <= ObSharedGCTask::MAX_VALUE_LENGTH) {
    blocksstable::MacroBlockId id(blocksstable::MacroBlockId::DEFAULT_FIRST_ID,
        static_cast<int64_t>(10000 + id_count), 0, 0, 0);
    int64_t next_serialize_size = 0;
    ASSERT_EQ(OB_SUCCESS, size_probe.get_serialize_size_after_add(
        id, probe_serialize_size, next_serialize_size));
    ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(id));
    ASSERT_EQ(OB_SUCCESS, size_probe.add(id));
    probe_serialize_size = next_serialize_size;
    ++id_count;
  }
  for (int64_t i = 0; i < EXTRA_OVERFLOW_ID_COUNT; ++i) {
    blocksstable::MacroBlockId id(blocksstable::MacroBlockId::DEFAULT_FIRST_ID,
        static_cast<int64_t>(10000 + id_count), 0, 0, 0);
    int64_t next_serialize_size = 0;
    ASSERT_EQ(OB_SUCCESS, size_probe.get_serialize_size_after_add(
        id, probe_serialize_size, next_serialize_size));
    ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(id));
    ASSERT_EQ(OB_SUCCESS, size_probe.add(id));
    probe_serialize_size = next_serialize_size;
    ++id_count;
  }
  ASSERT_GT(probe_serialize_size, ObSharedGCTask::MAX_VALUE_LENGTH);

  ObSharedGCTask src_row;
  src_row.key_ = ObSharedGCTaskKey(TENANT_ID, ObSharedGCTaskType::FAIL_GC,
                                   0/*round_id*/, FAIL_GC_TASK_ID);
  ObFailGCExecuteTask exec_task;
  ASSERT_EQ(OB_SUCCESS, exec_task.record_fail_gc_macro_ids_(src_row, macro_ids));

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT round_id, task_id, value FROM oceanbase.%s "
      "WHERE tenant_id = %lu AND type = %ld AND extra_info = 'fail_gc_task_id=%ld' "
      "ORDER BY task_id",
      OB_ALL_SHARED_GC_TASK_HISTORY_TNAME, TENANT_ID,
      static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC_MACRO_ID), FAIL_GC_TASK_ID));

  int64_t row_cnt = 0;
  int64_t decoded_cnt = 0;
  int64_t prev_task_id = -1;
  HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *r = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy().read(res, TENANT_ID, sql.ptr()));
    ASSERT_NE(nullptr, r = res.get_result());
    while (OB_SUCC(ret = r->next())) {
      int64_t round_id = -1;
      int64_t task_id = -1;
      ObString v;
      ASSERT_EQ(OB_SUCCESS, r->get_int("round_id", round_id));
      ASSERT_EQ(0, round_id);
      ASSERT_EQ(OB_SUCCESS, r->get_int("task_id", task_id));
      ASSERT_GT(task_id, prev_task_id);
      prev_task_id = task_id;
      ASSERT_EQ(OB_SUCCESS, r->get_varchar("value", v));
      ASSERT_LE(v.length(), ObSharedGCTask::MAX_VALUE_LENGTH);

      ObArenaAllocator chunk_alloc;
      ObString deep_v;
      ASSERT_EQ(OB_SUCCESS, ob_write_string(chunk_alloc, v, deep_v));
      ObSharedGCMacroIdList decoded;
      ASSERT_EQ(OB_SUCCESS, decoded.from_value(deep_v));
      ASSERT_GT(decoded.count(), 0);
      for (int64_t i = 0; i < decoded.count(); ++i) {
        ASSERT_LT(decoded_cnt, macro_ids.count());
        ASSERT_EQ(macro_ids.at(decoded_cnt).second_id(), decoded.at(i).second_id());
        ++decoded_cnt;
      }
      ++row_cnt;
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
  ASSERT_EQ(2, row_cnt);
  ASSERT_EQ(macro_ids.count(), decoded_cnt);

  ObSqlString del_sql;
  int64_t affected = 0;
  ASSERT_EQ(OB_SUCCESS, del_sql.assign_fmt(
      "DELETE FROM oceanbase.%s WHERE tenant_id = %lu AND type = %ld "
      "AND extra_info = 'fail_gc_task_id=%ld'",
      OB_ALL_SHARED_GC_TASK_HISTORY_TNAME, TENANT_ID,
      static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC_MACRO_ID), FAIL_GC_TASK_ID));
  ASSERT_EQ(OB_SUCCESS, sql_proxy().write(TENANT_ID, del_sql.ptr(), affected));
  ASSERT_EQ(row_cnt, affected);
}

} // namespace storage
} // namespace oceanbase

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
  GCONF.datafile_size.set_value("100G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
