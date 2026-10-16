/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef TEST_SHARED_STORAGE_SIMPLE_SERVER_STORAGE_CAHCE_COMMON_DEFINE_H_
#define TEST_SHARED_STORAGE_SIMPLE_SERVER_STORAGE_CAHCE_COMMON_DEFINE_H_
#endif

#include <gtest/gtest.h>

#define protected public
#define private public
#include "lib/literals/ob_literals.h"
#include "lib/utility/ob_test_util.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/shared_storage/test_ss_compaction_util.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"
#include "storage/shared_storage/prewarm/ob_storage_cache_policy_prewarmer.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/incremental/ob_shared_meta_service.h"

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

#ifdef __cplusplus
extern "C" const char *get_per_file_test_name();
#endif
namespace oceanbase
{
char *shared_storage_info = nullptr;
namespace unittest
{
bool scp_tenant_created = false;

struct TestRunCtx
{
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;

  TO_STRING_KV(K(tenant_id_), K(tenant_epoch_), K(ls_id_), K(ls_epoch_), K(tablet_id_));
};

TestRunCtx run_ctx_;

class ObStorageCachePolicyPrewarmerTest : public ObSimpleClusterTestBase
{
public:
  ObStorageCachePolicyPrewarmerTest()
      : ObSimpleClusterTestBase(get_per_file_test_name(), "50G", "50G", "50G")
  {}
  virtual void SetUp() override
  {
    if (!scp_tenant_created) {
      ObSimpleClusterTestBase::SetUp();
      OK(create_tenant_with_retry("tt1", "5G", "10G", false/*oracle_mode*/, 4));
      OK(get_tenant_id(run_ctx_.tenant_id_));
      ASSERT_NE(0, run_ctx_.tenant_id_);
      OK(get_curr_simple_server().init_sql_proxy2());
      share::ObTenantSwitchGuard tguard;
      ASSERT_EQ(OB_SUCCESS, tguard.switch_to(run_ctx_.tenant_id_));
      ObTenantDiskSpaceManager *tenant_disk_space_manager = MTL(ObTenantDiskSpaceManager *);
      ASSERT_NE(nullptr, tenant_disk_space_manager);
      OB_SERVER_DISK_SPACE_MGR.hidden_sys_data_disk_config_size_ = 1024;
      bool succ_resize = false;
      int64_t config_data_disk_size = 0;
      int64_t actual_data_disk_size = 0;
      ASSERT_EQ(OB_SUCCESS, tenant_disk_space_manager->get_tenant_unit_data_disk_size(
          MTL_ID(), config_data_disk_size, actual_data_disk_size));
      ASSERT_EQ(OB_SUCCESS, tenant_disk_space_manager->resize_total_disk_size_(config_data_disk_size, succ_resize));
      scp_tenant_created = true;
    }
    share::ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
    // Note: MTL_ID() returns sys tenant in SetUp context, use run_ctx_.tenant_id_ instead
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(run_ctx_.tenant_id_));
    ASSERT_TRUE(tenant_config.is_valid());
    tenant_config->_macro_cache_min_admit_block_size = 1;
  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

  void wait_major_finish();
  void wait_minor_finish();
  void wait_ss_minor_compaction_finish();
  void wait_task_finished(int64_t tablet_id);
  void set_ls_and_tablet_id_for_run_ctx(const char* table_name);
  void add_task(SCPTabletTaskMap &tablet_tasks, ObStorageCacheTaskStatusType status, int64_t end_time, const int64_t tablet_id);
  void check_macro_cache_exist();
  void exe_prepare_sql();
  void check_macro_blocks_type(const ObSSMacroCacheType expected_type);
  int get_shared_tablet(common::ObArenaAllocator &allocator, ObTabletHandle &tablet_handle);
  int get_shared_mini_minor_sstable_count(int64_t &mini_sstable_count, int64_t &minor_sstable_count);

};

static int exe_sql(const char *sql_str)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql str is null", KR(ret), KP(sql_str));
  } else if (OB_FAIL(sql.assign(sql_str))) {
    LOG_WARN("fail to assign sql", KR(ret), K(sql_str));
  } else if (OB_FAIL(ObStorageCachePolicyPrewarmerTest::get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to write sql", KR(ret), K(sql_str), K(sql));
  }
  return ret;
}

static int sys_exe_sql(const char *sql_str)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql str is null", KR(ret), KP(sql_str));
  } else if (OB_FAIL(sql.assign(sql_str))) {
    LOG_WARN("fail to assign sql", KR(ret), K(sql_str));
  } else if (OB_FAIL(ObStorageCachePolicyPrewarmerTest::get_curr_simple_server().get_sql_proxy().write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to write sql", KR(ret), K(sql_str), K(sql));
  }
  return ret;
}

class ObStorageCachePolicyColdTest : public ObStorageCachePolicyPrewarmerTest
{
public:
  ObStorageCachePolicyColdTest()
      : ObStorageCachePolicyPrewarmerTest()
  {}
  virtual ~ObStorageCachePolicyColdTest() {}
};

void ObStorageCachePolicyPrewarmerTest::wait_major_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait major begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  ObSqlString sql;
  int64_t affected_rows = 0;
  static int64_t old_major_scn = 1;
  int64_t new_major_scn = 1;
  int64_t scn = 0;
  do {
    OK(sql.assign_fmt(
        "select frozen_scn, (frozen_scn - last_scn) as result"
        " from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id=%lu;",
        run_ctx_.tenant_id_));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      OK(sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      OK(result->next());
      OK(result->get_int("result", scn));
      OK(result->get_int("frozen_scn", new_major_scn));
    }
    LOG_INFO("major result", K(scn), K(new_major_scn));
    ob_usleep(100 * 1000); // 100_ms
  } while (0 != scn || old_major_scn == new_major_scn);

  old_major_scn = new_major_scn;
  LOG_INFO("major finished", K(new_major_scn));
}

void ObStorageCachePolicyPrewarmerTest::wait_minor_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait minor begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=0;",
              run_ctx_.tenant_id_, run_ctx_.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    usleep(100 * 1000);
    LOG_INFO("minor result", K(row_cnt));
  } while (row_cnt > 0);
  LOG_INFO("minor finished", K(row_cnt));
}

void ObStorageCachePolicyPrewarmerTest::wait_ss_minor_compaction_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait ss minor compaction begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t row_cnt = 0;
  const int64_t timeout_us = 300 * 1000 * 1000L;
  int64_t wait_start_ts = ObTimeUtility::current_time();
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=12;",
              run_ctx_.tenant_id_, run_ctx_.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    usleep(100 * 1000);
    LOG_INFO("minor result", K(row_cnt));
    ASSERT_LT(ObTimeUtility::current_time() - wait_start_ts, timeout_us);
  } while (row_cnt > 0);

  int64_t mini_sstable_count = 0;
  int64_t minor_sstable_count = 0;
  wait_start_ts = ObTimeUtility::current_time();
  do {
    ret = get_shared_mini_minor_sstable_count(mini_sstable_count, minor_sstable_count);
    if (OB_TABLET_NOT_EXIST == ret) {
      mini_sstable_count = 0;
      minor_sstable_count = 0;
      LOG_INFO("shared tablet does not exist yet", KR(ret), K(run_ctx_));
    } else {
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    usleep(100 * 1000);
    LOG_INFO("shared ss minor compaction result", K(run_ctx_), K(mini_sstable_count), K(minor_sstable_count));
    ASSERT_LT(ObTimeUtility::current_time() - wait_start_ts, timeout_us);
  } while (mini_sstable_count > 0 || minor_sstable_count <= 0);
  LOG_INFO("wait ss minor compaction finished", K(run_ctx_), K(mini_sstable_count), K(minor_sstable_count));
}

int ObStorageCachePolicyPrewarmerTest::get_shared_tablet(
    common::ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *ss_meta_service = MTL(ObSSMetaService *);
  if (OB_ISNULL(ss_meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss meta service is null", KR(ret), K(run_ctx_));
  } else if (OB_FAIL(ss_meta_service->get_tablet(run_ctx_.ls_id_,
                                                 run_ctx_.tablet_id_,
                                                 share::SCN::min_scn(),
                                                 allocator,
                                                 tablet_handle))) {
    LOG_WARN("fail to get shared tablet", KR(ret), K(run_ctx_));
  }
  return ret;
}

int ObStorageCachePolicyPrewarmerTest::get_shared_mini_minor_sstable_count(
    int64_t &mini_sstable_count,
    int64_t &minor_sstable_count)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "GetSSMinorCnt"));
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTableStoreIterator table_iter;
  ObITable *table = nullptr;

  mini_sstable_count = 0;
  minor_sstable_count = 0;
  if (OB_FAIL(get_shared_tablet(allocator, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("fail to get shared tablet", KR(ret), K(run_ctx_));
    }
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared tablet handle is invalid", KR(ret), K(run_ctx_), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared tablet is null", KR(ret), K(run_ctx_), K(tablet_handle));
  } else if (OB_FAIL(tablet->get_mini_minor_sstables(table_iter))) {
    LOG_WARN("fail to get shared mini minor sstables", KR(ret), K(run_ctx_), KPC(tablet));
  } else {
    while (OB_SUCC(ret)) {
      table = nullptr;
      if (OB_FAIL(table_iter.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next shared mini minor sstable", KR(ret), K(run_ctx_));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared mini minor sstable is null", KR(ret), K(run_ctx_));
      } else if (table->is_mini_sstable()) {
        ++mini_sstable_count;
      } else if (table->is_minor_sstable()) {
        ++minor_sstable_count;
      }
    }
  }
  LOG_INFO("get shared mini minor sstable count",
           KR(ret), K(run_ctx_), K(mini_sstable_count), K(minor_sstable_count));
  return ret;
}

void ObStorageCachePolicyPrewarmerTest::wait_task_finished(int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  static constexpr int64_t WAIT_TASK_FINISHED_TIMEOUT_US = 2_min;
  const int64_t start_time_us = ObTimeUtility::current_time_us();
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  ObSqlString sql;
  int64_t count = 0;
  FLOG_INFO("[TEST] start to wait task finished", K(run_ctx_), K(tablet_id));
  do {
    OK(sql.assign_fmt(
        "select count(*) as count from oceanbase.__all_virtual_storage_cache_task where tablet_id=%lu and status = 'FINISHED';",
        run_ctx_.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      OK(sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      OK(result->next());
      OK(result->get_int("count", count));
    }
    ob_usleep(100_ms);
  } while (0 == count
      && ObTimeUtility::current_time_us() - start_time_us < WAIT_TASK_FINISHED_TIMEOUT_US);
  const int64_t wait_time_us = ObTimeUtility::current_time_us() - start_time_us;
  FLOG_INFO("[TEST] finish waiting storage cache task",
      K(tablet_id), K(count), K(wait_time_us), K(run_ctx_));
  ASSERT_GT(count, 0);
}

void ObStorageCachePolicyPrewarmerTest::set_ls_and_tablet_id_for_run_ctx(const char* table_name)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tablet_id = 0;
  OK(sql.assign_fmt("select tablet_id from oceanbase.__all_virtual_table where table_name='%s';", table_name));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    OK(result->next());
    OK(result->get_uint("tablet_id", tablet_id));
  }
  run_ctx_.tablet_id_ = tablet_id;

  sql.reset();
  int64_t id = 0;
  OK(sql.assign_fmt("select ls_id from oceanbase.__all_tablet_to_ls where tablet_id=%ld;", tablet_id));
  SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res2, sql.ptr()));
    sqlclient::ObMySQLResult *result = res2.get_result();
    ASSERT_NE(nullptr, result);
    OK(result->next());
    OK(result->get_int("ls_id", id));
  }
  ObLSID ls_id(id);

  ObLSHandle ls_handle;
  OK(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::TXSTORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  run_ctx_.ls_id_ = ls->get_ls_id();
  run_ctx_.ls_epoch_ = ls->get_ls_epoch();
  run_ctx_.tenant_epoch_ = MTL_EPOCH_ID();
  LOG_INFO("[TEST] finish set run ctx", K(run_ctx_), K(table_name));
}

void ObStorageCachePolicyPrewarmerTest::add_task(SCPTabletTaskMap &tablet_tasks, ObStorageCacheTaskStatusType status, int64_t end_time, const int64_t tablet_id)
{
  ObStorageCacheTabletTask *task = nullptr;
  task = OB_NEW(ObStorageCacheTabletTask, "ObStorageCache");
  ASSERT_NE(nullptr, task);
  ObStorageCacheTabletTaskHandle handle;
  ASSERT_EQ(OB_SUCCESS, task->init_for_test(run_ctx_.tenant_id_, 2, tablet_id, PolicyStatus::HOT));
  task->status_ = status;

  task->set_end_time(end_time);
  handle.set_ptr(task);
  ASSERT_EQ(OB_SUCCESS, tablet_tasks.set_refactored(task->get_tablet_id(), handle));
}

void ObStorageCachePolicyPrewarmerTest::check_macro_cache_exist()
{
  ObSEArray<MacroBlockId, 64> data_block_ids;
  ObSEArray<MacroBlockId, 16> meta_block_ids;
  FLOG_INFO("[TEST] start to check macro cache", K(run_ctx_));
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));

  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
  FLOG_INFO("[TEST] current info", K(run_ctx_), K(data_block_ids), K(meta_block_ids));

  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  bool is_exist_in_macro_cache = false;
  bool is_exist_in_kvcache = false;
  wait_task_finished(run_ctx_.tablet_id_.id());
  for (int i = 0; i < data_block_ids.count(); i++) {
    is_exist_in_macro_cache = false;
    is_exist_in_kvcache = false;
    FLOG_INFO("[TEST] macro id", K(data_block_ids.at(i)), K(i));
    if (data_block_ids.at(i).is_data() && data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist_in_macro_cache));
      OK(tenant_kvcache_mgr->check_exist(data_block_ids.at(i), is_exist_in_kvcache));
      if (!is_exist_in_macro_cache && !is_exist_in_kvcache) {
        FLOG_INFO("macro is not exist", K(i), K(run_ctx_), K(data_block_ids.at(i)));
      }
      ASSERT_TRUE(is_exist_in_macro_cache || is_exist_in_kvcache);
    }
  }
  LOG_INFO("[TEST] finish to check macro cache exist", K(run_ctx_));
}

void ObStorageCachePolicyPrewarmerTest::exe_prepare_sql()
{
  // shared storage 上传调度周期，调小到 1s，数据更快从本地写缓存推进到对象存储
  OK(exe_sql("alter system set _shared_inc_sstable_upload_interval = '1s';"));
  // 增量 sstable 上传线程的调度权重/优先级，提高后增量 SSTable 上传更积极
  OK(exe_sql("alter system set inc_sstable_upload_thread_score = 20;"));
  // shared storage 垃圾回收周期，调小后，GC 更频繁执行
  OK(exe_sql("alter system set _shared_gc_interval = '10s';"));
  // shared storage GC 时判定文件可回收的时间阈值
  OK(sys_exe_sql("alter system set _shared_gc_file_expiration_time = '10s';"));
  // shared storage 相关 checkpoint 推进周期
  OK(exe_sql("alter system set _shared_advance_checkpoint_interval = '1m'"));
  // 租户 tt1 的 compaction 调度周期
  OK(sys_exe_sql("alter system set ob_compaction_schedule_interval = '3s' tenant tt1;"));
  // 租户 tt1 的合并检查周期
  OK(sys_exe_sql("alter system set merger_check_interval = '10s' tenant tt1;"));
  // 租户 tt1 的 minor compaction 触发阈值
  OK(sys_exe_sql("alter system set minor_compact_trigger = 2 tenant tt1;"));
  // 租户 tt1 的 major 后预热级别
  OK(sys_exe_sql("alter system set _major_compaction_prewarm_level = 0 tenant tt1;"));
  OK(sys_exe_sql("alter system set_tp tp_name = EN_COMPACTION_SS_MINOR_MERGE_FAST_SKIP, error_code = 4016, frequency = 1;"));
  OK(sys_exe_sql("alter system set _shared_advance_checkpoint_interval = '1m';"));
  FLOG_INFO("set _major_compaction_prewarm_level = 0");
}

void ObStorageCachePolicyPrewarmerTest::check_macro_blocks_type(const ObSSMacroCacheType expected_type)
{
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 128> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "BlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
  FLOG_INFO("[TEST] start to check macro blocks type", K(run_ctx_), K(expected_type), K(data_block_ids), K(meta_block_ids));
  bool is_exist = false;
  ObSSMacroCacheMgr *ss_cache_mgr = MTL(ObSSMacroCacheMgr*);
  ASSERT_NE(nullptr, ss_cache_mgr);
  ObSSMacroCacheMetaHandle meta_handle;
  for (int i = 0; i < data_block_ids.count(); i++) {
    is_exist = false;
    meta_handle.reset();
    OK(ss_cache_mgr->get_macro_cache_meta(data_block_ids.at(i), is_exist, meta_handle));
    if (is_exist) {
      if (expected_type != meta_handle()->cache_type_) {
        FLOG_INFO("[TEST] data macro id", K(data_block_ids.at(i)), K(i), K(expected_type));
      }
      ASSERT_EQ(expected_type, meta_handle()->cache_type_);
    }
  }
  FLOG_INFO("[TEST] finish check all macro blocks type", K(run_ctx_), K(expected_type));
}
}
}
