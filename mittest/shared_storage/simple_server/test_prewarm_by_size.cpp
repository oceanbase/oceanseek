/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGETEST
#define protected public
#define private public
#define UNITTEST

#include "storage/tx_storage/ob_ls_service.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "share/ob_ls_id.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "common/ob_tablet_id.h"
#include "lib/literals/ob_literals.h"
#include "lib/utility/ob_defer.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "lib/utility/ob_test_util.h"
#include "storage/shared_storage/task/ob_ss_preread_task.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"
#include "storage/shared_storage/prewarm/ob_ss_base_prewarmer.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "src/storage/incremental/ob_shared_meta_service.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "logservice/palf/election/utils/election_common_define.h"
#include "mittest/shared_storage/test_ss_compaction_util.h"
#include "mittest/shared_storage/simple_server/test_storage_cache_common_util.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace sslog
{


} // namespace sslog
} // namespace oceanbase

namespace oceanbase
{
bool scp_tenant_created = false;
namespace unittest
{

using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

struct Testrun_ctx
{
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;
  int64_t time_sec_ = 0;

  TO_STRING_KV(K(tenant_id_), K(tenant_epoch_), K(ls_id_), K(ls_epoch_), K(tablet_id_));
};

Testrun_ctx run_ctx;

class ObPrereadMetaMacroTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObPrereadMetaMacroTest() : ObSimpleClusterTestBase("test_preread_meta_macro_", "50G", "50G", "50G")
  {}
   virtual void SetUp() override
  {
    if (!scp_tenant_created) {
      ObSimpleClusterTestBase::SetUp();
      OK(create_tenant_with_retry("tt1", "5G", "10G", false/*oracle_mode*/, 8));
      OK(get_tenant_id(run_ctx.tenant_id_));
      ASSERT_NE(0, run_ctx.tenant_id_);
      OK(get_curr_simple_server().init_sql_proxy2());
      scp_tenant_created = true;
      // Wait for tenant services to be ready, this helps avoid LS leader election issues
      {
        share::ObTenantSwitchGuard tguard;
        OK(tguard.switch_to(run_ctx.tenant_id_));
        OK(TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
      }
    }
  }
  void set_ls_and_tablet_id_for_run_ctx(const char *table_name);
  void exe_prepare_sql();
  void wait_minor_finish();
  void wait_ss_minor_compaction_finish();
  int get_shared_tablet(common::ObArenaAllocator &allocator, ObTabletHandle &tablet_handle);
  int get_shared_mini_minor_sstable_count(int64_t &mini_sstable_count, int64_t &minor_sstable_count);
  void reset_read_info();
  void check_macro_exist_in_local_cache(
      const MacroBlockId &macro_id, const char *macro_buf, const int64_t macro_size, const bool is_hot_tablet);
  void check_macro_exist_in_macro_cache(const int64_t tablet_id, int64_t &macro_in_macro_cache_cnt);
  void get_macro_cnt_in_local_cache(
      const int64_t tablet_id, const bool is_hot_tablet, int64_t &in_macro_cache_cnt, int64_t &skipped_macro_cnt);
  void get_shared_blocks_for_tablet(common::ObIArray<blocksstable::MacroBlockId> &block_ids);
  int check_local_cache(const MacroBlockId &macro_id, const bool expect_exist, const bool is_hot_tablet);
  void wait_preread_task_finish();
private:
  static int cancel_and_wait_compaction_(
      const common::ObIArray<share::ObLSID> &ls_ids);
public:
  static void TearDownTestCase()
    {
      ResidualDataCleanerHelper::clean_in_mock_env();
      ObSimpleClusterTestBase::TearDownTestCase();
    }
public:
  static const int64_t READ_IO_SIZE = DIO_READ_ALIGN_SIZE * 256 * 2; // 2MB
protected:
  char read_buf_[READ_IO_SIZE];
  ObStorageObjectReadInfo read_info_;
};

int ObPrereadMetaMacroTest::cancel_and_wait_compaction_(
    const common::ObIArray<share::ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  static constexpr int64_t WAIT_TIMEOUT_US = 300_s;
  static constexpr int64_t CHECK_INTERVAL_US = 100_ms;
  ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler *);
  const int64_t start_time_us = ObTimeUtility::current_time_us();
  int64_t check_round = 0;
  bool has_compaction = true;
  LOG_INFO("start to cancel and wait compaction", K(ls_ids));
  if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dag scheduler is null", KR(ret), K(ls_ids));
  }
  while (OB_SUCC(ret) && has_compaction) {
    has_compaction = false;
    ++check_round;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_ids.count(); ++i) {
      bool ls_has_compaction = false;
      if (OB_FAIL(dag_scheduler->check_ls_compaction_dag_exist_with_cancel(
          ls_ids.at(i), ls_has_compaction))) {
        LOG_WARN("fail to cancel LS compaction",
            KR(ret), K(i), K(ls_ids.at(i)), K(check_round));
      } else {
        has_compaction = has_compaction || ls_has_compaction;
      }
    }
    const int64_t elapsed_time_us = ObTimeUtility::current_time_us() - start_time_us;
    if (OB_SUCC(ret) && has_compaction && elapsed_time_us >= WAIT_TIMEOUT_US) {
      ret = OB_TIMEOUT;
      LOG_WARN("cancel and wait compaction timeout",
          KR(ret), K(elapsed_time_us), K(check_round), K(ls_ids));
    } else if (OB_SUCC(ret) && has_compaction) {
      ob_usleep(CHECK_INTERVAL_US);
    }
  }
  const int64_t elapsed_time_us = ObTimeUtility::current_time_us() - start_time_us;
  LOG_INFO("finish to cancel and wait compaction",
      KR(ret), K(elapsed_time_us), K(check_round), K(ls_ids));
  return ret;
}

void ObPrereadMetaMacroTest::set_ls_and_tablet_id_for_run_ctx(const char* table_name)
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
  run_ctx.tablet_id_ = tablet_id;

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
  run_ctx.ls_id_ = ls->get_ls_id();
  run_ctx.ls_epoch_ = ls->get_ls_epoch();
  run_ctx.tenant_epoch_ = MTL_EPOCH_ID();
  LOG_INFO("[TEST] finish set run ctx", K(run_ctx), K(table_name));
}

void ObPrereadMetaMacroTest::exe_prepare_sql()
{
  int ret = OB_SUCCESS;
  OK(exe_sql("alter system set _shared_inc_sstable_upload_interval = '1s';"));
  OK(exe_sql("alter system set inc_sstable_upload_thread_score = 20;"));
  OK(sys_exe_sql("alter system set _shared_gc_file_expiration_time = '10s';"));
  OK(sys_exe_sql("alter system set ob_compaction_schedule_interval = '3s' tenant tt1;"));
  OK(sys_exe_sql("alter system set minor_compact_trigger = 2 tenant tt1;"));
  OK(sys_exe_sql("alter system set _macro_cache_miss_threshold_for_prefetch = 1 tenant tt1;"));
  OK(sys_exe_sql("alter system set_tp tp_name = EN_COMPACTION_SS_MINOR_MERGE_FAST_SKIP, error_code = 4016, frequency = 1;"));
  sleep(10);
}

void ObPrereadMetaMacroTest::wait_minor_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait minor begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=0;",
              run_ctx.tenant_id_, run_ctx.tablet_id_.id()));
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

int ObPrereadMetaMacroTest::get_shared_tablet(
    common::ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *ss_meta_service = MTL(ObSSMetaService *);
  if (OB_ISNULL(ss_meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss meta service is null", KR(ret), K(run_ctx));
  } else if (OB_FAIL(ss_meta_service->get_tablet(run_ctx.ls_id_,
                                                 run_ctx.tablet_id_,
                                                 share::SCN::min_scn(),
                                                 allocator,
                                                 tablet_handle))) {
    LOG_WARN("fail to get shared tablet", KR(ret), K(run_ctx));
  }
  return ret;
}

int ObPrereadMetaMacroTest::get_shared_mini_minor_sstable_count(
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
      LOG_WARN("fail to get shared tablet", KR(ret), K(run_ctx));
    }
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared tablet handle is invalid", KR(ret), K(run_ctx), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared tablet is null", KR(ret), K(run_ctx), K(tablet_handle));
  } else if (OB_FAIL(tablet->get_mini_minor_sstables(table_iter))) {
    LOG_WARN("fail to get shared mini minor sstables", KR(ret), K(run_ctx), KPC(tablet));
  } else {
    while (OB_SUCC(ret)) {
      table = nullptr;
      if (OB_FAIL(table_iter.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next shared mini minor sstable", KR(ret), K(run_ctx));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared mini minor sstable is null", KR(ret), K(run_ctx));
      } else if (table->is_mini_sstable()) {
        ++mini_sstable_count;
      } else if (table->is_minor_sstable()) {
        ++minor_sstable_count;
      }
    }
  }
  LOG_INFO("get shared mini minor sstable count",
           KR(ret), K(run_ctx), K(mini_sstable_count), K(minor_sstable_count));
  return ret;
}

void ObPrereadMetaMacroTest::wait_ss_minor_compaction_finish()
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
              run_ctx.tenant_id_, run_ctx.tablet_id_.id()));
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
      LOG_INFO("shared tablet does not exist yet", KR(ret), K(run_ctx));
    } else {
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    usleep(100 * 1000);
    LOG_INFO("shared ss minor compaction result", K(run_ctx), K(mini_sstable_count), K(minor_sstable_count));
    ASSERT_LT(ObTimeUtility::current_time() - wait_start_ts, timeout_us);
  } while (mini_sstable_count > 0 || minor_sstable_count <= 0);
  LOG_INFO("wait ss minor compaction finished", K(run_ctx), K(mini_sstable_count), K(minor_sstable_count));
}

void ObPrereadMetaMacroTest::check_macro_exist_in_macro_cache(const int64_t tablet_id, int64_t &macro_in_macro_cache_cnt)
{
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  bool is_exist = false;
  FLOG_INFO("[TEST] check macro exist in macro cache", K(tablet_id));

  ObArray<blocksstable::MacroBlockId> block_ids;
  get_shared_blocks_for_tablet(block_ids);

  FLOG_INFO("[TEST] all macro blocks", K(block_ids));
  for (int i = 0; i < block_ids.count(); i++) {
    MacroBlockId data_block_id = block_ids.at(i);
    if (data_block_id.is_shared_data_or_meta()) {
      FLOG_INFO("[TEST] data block id", K(data_block_id));
      OK(macro_cache_mgr->exist(data_block_id, is_exist));
      if (is_exist) {
        macro_in_macro_cache_cnt++;
        FLOG_INFO("[TEST] exist data block in macro cache", K(data_block_id));
      }
    } 
  } 
  FLOG_INFO("[TEST] data block cnt in macro cache", K(macro_in_macro_cache_cnt));
}

void ObPrereadMetaMacroTest::get_shared_blocks_for_tablet(
    common::ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  bool is_old_version_empty = false;
  int64_t current_tablet_version = -1;
  int64_t current_tablet_trans_seq = -1;
  int64_t reorganization_scn = 0;
  int64_t last_gc_version = -1;
  uintptr_t tablet_fingerprint = 0;
  bool is_transfer_out_deleted = false;
  bool is_set_stop = false;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantMetaMemMgr*)->get_current_version_for_tablet(run_ctx.ls_id_,
            run_ctx.tablet_id_, current_tablet_version, last_gc_version, current_tablet_trans_seq,
            tablet_fingerprint, is_old_version_empty, is_transfer_out_deleted,
            reorganization_scn));
  ASSERT_NE(-1, current_tablet_version);

  ObPrivateBlockGCHandler handler(run_ctx.ls_id_, run_ctx.ls_epoch_, run_ctx.tablet_id_,
                                  current_tablet_version, last_gc_version,
                                  reorganization_scn,
                                  tablet_fingerprint,
                                  is_set_stop, is_transfer_out_deleted);
  ASSERT_EQ(OB_SUCCESS, handler.get_blocks_for_tablet(current_tablet_version, true/*is_shared*/, block_ids));
  LOG_INFO("get shared blocks for tablet", K(block_ids));
}

int ObPrereadMetaMacroTest::check_local_cache(
    const MacroBlockId &macro_id, const bool expect_exist, const bool is_hot_tablet)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicyPrewarmer macro_reader(1/*parallelism*/);
  if (OB_FAIL(macro_reader.async_read_from_object_storage_(macro_id))) {
    LOG_WARN("fail to read meta macro", KR(ret), K(macro_id), K(expect_exist));
  } else if (OB_FAIL(macro_reader.batch_read_wait_())) {
    LOG_WARN("fail to wait read", KR(ret), K(macro_id), K(expect_exist));
  } else {
    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    PrewarmLocalCacheType ss_prewarm_local_cache_type = PrewarmLocalCacheType::SS_PREWARM_MAX_TYPE;
    macro_reader.check_prewarm_cache_type(macro_reader.read_handles_[0].get_macro_id(), macro_reader.read_handles_[0].get_buffer(),
        macro_reader.read_handles_[0].get_data_size(), is_hot_tablet, ss_prewarm_local_cache_type);
    if(OB_ISNULL(macro_cache_mgr)) {
      LOG_INFO("skip shared object meta magic header macro",
          K(macro_id), K(macro_reader.read_handles_[0]));
    } else if (ObSSPrewarmLocalCacheType::is_skipped(ss_prewarm_local_cache_type)) {
      LOG_INFO("skip shared object meta magic header macro",
          K(macro_id), K(macro_reader.read_handles_[0]));
    } else if (ObSSPrewarmLocalCacheType::is_macro_cache(ss_prewarm_local_cache_type)) {
      bool is_hit = false;
      ObSSFdCacheHandle fd_handle;
      if(OB_FAIL(macro_cache_mgr->get(macro_id, run_ctx.tablet_id_, 0/*hit size*/, is_hit, fd_handle))) {
        LOG_WARN("fail to get macro cache", KR(ret), K(macro_id));
      } else {
        LOG_INFO("check macro cache", K(macro_id), K(is_hit), K(expect_exist));
        if (expect_exist != is_hit) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect_exist is not equal to is_hit", KR(ret), K(macro_id), K(expect_exist), K(is_hit));
        }
      }
      FLOG_INFO("[TEST] check macro cache", K(macro_id), K(is_hit), K(expect_exist));
    }
  }
  return ret;
}

void ObPrereadMetaMacroTest::reset_read_info()
{
  read_buf_[0] = '\0';
  read_info_.io_desc_.set_wait_event(1); 
  read_info_.buf_ = read_buf_;
  read_info_.offset_ = 0;
  read_info_.size_ = READ_IO_SIZE;
  read_info_.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info_.mtl_tenant_id_ = run_ctx.tenant_id_;
  read_info_.set_effective_tablet_id(run_ctx.tablet_id_);
}

void ObPrereadMetaMacroTest::check_macro_exist_in_local_cache(
    const MacroBlockId &macro_block_id, const char *macro_buf, const int64_t macro_size, const bool is_hot_tablet)
{
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  ObSSBasePrewarmer base_prewarmer;
  PrewarmLocalCacheType ss_prewarm_local_cache_type = PrewarmLocalCacheType::SS_PREWARM_MAX_TYPE;
  FLOG_INFO("[TEST] macro block id", K(macro_block_id));
  if (macro_block_id.is_shared_data_or_meta()) {
    base_prewarmer.check_prewarm_cache_type(
        macro_block_id, macro_buf, macro_size, is_hot_tablet, ss_prewarm_local_cache_type);
    FLOG_INFO("[TEST] object size", K(macro_block_id), K(macro_size), K(is_hot_tablet),
        K(ss_prewarm_local_cache_type));
    if (ObSSPrewarmLocalCacheType::is_macro_cache(ss_prewarm_local_cache_type)) {
      bool is_exist_in_macro_cache = false;
      bool is_exist_in_kvcache = false;
      ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->exist(macro_block_id, is_exist_in_macro_cache));
      ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->check_exist(macro_block_id, is_exist_in_kvcache));
      if (!(is_exist_in_macro_cache || is_exist_in_kvcache)) {
        FLOG_INFO("[TEST] macro is not exist", K(run_ctx), K(macro_block_id), K(macro_size),
            K(is_hot_tablet), K(is_exist_in_macro_cache), K(is_exist_in_kvcache));
      }
      ASSERT_TRUE(is_exist_in_macro_cache || is_exist_in_kvcache);
    }
    FLOG_INFO("[TEST] check macro exist end", K(macro_block_id), K(macro_size),
        K(is_hot_tablet), K(ss_prewarm_local_cache_type));
  }
}

void ObPrereadMetaMacroTest::get_macro_cnt_in_local_cache(
    const int64_t tablet_id, const bool is_hot_tablet, int64_t &in_macro_cache_cnt, int64_t &skipped_macro_cnt)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ObSSBasePrewarmer base_prewarmer;
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObSEArray<MacroBlockId, 64> data_block_ids;
  ObSEArray<MacroBlockId, 64> meta_block_ids;
  const ObTabletID target_tablet_id(tablet_id);

  data_block_ids.set_attr(ObMemAttr(run_ctx.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx.tenant_id_, "MetaBlockIDs"));

  OK(get_macro_blocks(run_ctx.ls_id_, target_tablet_id, data_block_ids, meta_block_ids));
  sleep(20);
  for (int i = 0; i < data_block_ids.count(); i++) {
    MacroBlockId data_block_id = data_block_ids.at(i);
    int64_t object_size = 0;
    if (data_block_id.is_shared_data_or_meta()) {
      PrewarmLocalCacheType ss_prewarm_local_cache_type = PrewarmLocalCacheType::SS_PREWARM_MAX_TYPE;
      FLOG_INFO("[TEST] data block id in get_macro_cnt_in_local_cache", K(data_block_id));
      OK(OB_STORAGE_OBJECT_MGR.get_object_size(data_block_id, run_ctx.ls_epoch_, object_size));
      base_prewarmer.check_prewarm_cache_type(
          data_block_id, nullptr, object_size, is_hot_tablet, ss_prewarm_local_cache_type);
      if (ObSSPrewarmLocalCacheType::is_macro_cache(ss_prewarm_local_cache_type)) {
        in_macro_cache_cnt++;
        FLOG_INFO("[TEST] should data block id in macro block", K(in_macro_cache_cnt), K(skipped_macro_cnt),
            K(data_block_id), K(object_size), K(is_hot_tablet));
      } else {
        skipped_macro_cnt++;
        FLOG_INFO("[TEST] should skip data block", K(in_macro_cache_cnt), K(skipped_macro_cnt),
            K(data_block_id), K(object_size), K(is_hot_tablet));
      }
      FLOG_INFO("[TEST] get data macro in local cache", K(in_macro_cache_cnt), K(skipped_macro_cnt),
          K(data_block_id), K(object_size), K(is_hot_tablet), K(ss_prewarm_local_cache_type));
    }
  }
  for (int i = 0; i < meta_block_ids.count(); i++) {
    MacroBlockId meta_block_id = meta_block_ids.at(i);
    int64_t object_size = 0;
    if (meta_block_id.is_shared_data_or_meta()) {
      PrewarmLocalCacheType ss_prewarm_local_cache_type = PrewarmLocalCacheType::SS_PREWARM_MAX_TYPE;
      FLOG_INFO("[TEST] meta block id in get_macro_cnt_in_local_cache", K(meta_block_id));
      OK(OB_STORAGE_OBJECT_MGR.get_object_size(meta_block_id, run_ctx.ls_epoch_, object_size));
      base_prewarmer.check_prewarm_cache_type(
          meta_block_id, nullptr, object_size, is_hot_tablet, ss_prewarm_local_cache_type);
      if (ObSSPrewarmLocalCacheType::is_macro_cache(ss_prewarm_local_cache_type)) {
        in_macro_cache_cnt++;
        FLOG_INFO("[TEST] should meta block id in macro block", K(in_macro_cache_cnt), K(skipped_macro_cnt),
            K(meta_block_id), K(object_size), K(is_hot_tablet));
      } else {
        skipped_macro_cnt++;
        FLOG_INFO("[TEST] should skip meta block", K(in_macro_cache_cnt), K(skipped_macro_cnt),
            K(meta_block_id), K(object_size), K(is_hot_tablet));
      }
      FLOG_INFO("[TEST] get meta macro in local cache", K(in_macro_cache_cnt), K(skipped_macro_cnt),
          K(meta_block_id), K(object_size), K(is_hot_tablet), K(ss_prewarm_local_cache_type));
    }
  }
}

void ObPrereadMetaMacroTest::wait_preread_task_finish()
{
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;

  while ((preread_cache_mgr.preread_queue_.size() != 0) ||
         (preread_task.segment_files_.count() != 0) ||
         (preread_task.async_read_list_.get_curr_total() != 0) ||
         (preread_task.free_list_.get_curr_total() != preread_task.max_pre_read_parallelism_)) {
          sleep(1);
          FLOG_INFO("[TEST] try to wait preread task finish", K(preread_cache_mgr.preread_queue_.size()), 
            K(preread_task.segment_files_.count()), K(preread_task.async_read_list_.get_curr_total()), 
            K(preread_task.free_list_.get_curr_total()));
  }
  FLOG_INFO("[TEST] wait preread task finish end", K(preread_task));
}

// Disabled: preread micro cache check is unreliable after local minor gating removed in inc-upload refactor.
TEST_F(ObPrereadMetaMacroTest, DISABLED_test_preread_task)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx.tenant_id_));
  FLOG_INFO("[TEST] start test_preread_meta_macro", K(run_ctx));
  OK(sys_exe_sql("alter system set _major_compaction_prewarm_level = 0 tenant tt1;"));
  exe_prepare_sql();
  // 1. create table and start medium compact

  OK(exe_sql("create table test_macro_cache (a bigint)"));
  set_ls_and_tablet_id_for_run_ctx("test_macro_cache");
  OK(exe_sql("insert into test_macro_cache select random(100) from table(generator(262144))"));
  sleep(1);
  OK(TestCompactionUtil::medium_compact(run_ctx.tenant_id_, run_ctx.tablet_id_.id(), run_ctx.ls_id_));
  FLOG_INFO("[TEST] finish medium compact (test_preread_task)", K(run_ctx.tablet_id_));
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  // 2. get macro cache meta block ids
  ObArray<blocksstable::MacroBlockId> block_ids;
  get_shared_blocks_for_tablet(block_ids);

  FLOG_INFO("[TEST] preread data block begin");
  MacroBlockId data_block_id;
  ObStorageObjectHandle read_object_handle;

  // preread file to trigger preread task for data block
  for (int i = 0; i < block_ids.count(); i++) {
    FLOG_INFO("[TEST] data block id", K(block_ids.at(i)), K(i));
    if (block_ids.at(i).is_shared_data_or_meta()) {
      // 2. read to trigger macro cache miss
      read_object_handle.reset();
      reset_read_info();
      data_block_id.reset();
      data_block_id = block_ids.at(i);
      read_info_.macro_block_id_ = data_block_id;
      ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
      ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
      FLOG_INFO("[TEST] finish async pread file", K(read_info_), K(data_block_id), K(read_object_handle.get_data_size()));
      ASSERT_NE(nullptr, read_object_handle.get_buffer());
      ASSERT_GE(read_object_handle.get_data_size(), 0);
      wait_preread_task_finish();
      FLOG_INFO("[TEST] finish wait preread task", K(data_block_id));
      check_macro_exist_in_local_cache(
          data_block_id, read_object_handle.get_buffer(), read_object_handle.get_data_size(), false/*is_hot_tablet*/);
    }
  }  
  FLOG_INFO("[TEST] finish test_preread_meta_macro", K(run_ctx));
}


// Disabled: depends on ObSSMinorAttachHandler::do_prewarm_() which was removed in inc-upload refactor.
TEST_F(ObPrereadMetaMacroTest, DISABLED_test_minor_compact_prewarm)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx.tenant_id_));
  FLOG_INFO("[TEST] start test_minor_compact_prewarm", K(run_ctx));
  exe_prepare_sql();
  OK(sys_exe_sql("alter system set _major_compaction_prewarm_level = 0 tenant tt1;"));
  sleep(60);
  OK(exe_sql("create table test_minor_compact_prewarm (c1 varchar(1024), c2 varchar(1024), c3 varchar(1024), c4 varchar(1024))"));
  set_ls_and_tablet_id_for_run_ctx("test_minor_compact_prewarm");
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  preread_task.is_inited_ = false;
  // trigger minor compaction by minor freeze 3 times. it prewarms minor sstable into local
  // macro cache, and update tablet table store.

  OK(exe_sql("insert into test_minor_compact_prewarm select randstr(1024, random()) c1, randstr(1024, random()) c2, randstr(1024, random()) c3, randstr(1024, random()) c4 from table(generator(10000));"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_minor_compact_prewarm select randstr(1024, random()) c1, randstr(1024, random()) c2, randstr(1024, random()) c3, randstr(1024, random()) c4 from table(generator(10000));"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_minor_compact_prewarm select randstr(1024, random()) c1, randstr(1024, random()) c2, randstr(1024, random()) c3, randstr(1024, random()) c4 from table(generator(10000));"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();
  
  sleep(20);
  wait_ss_minor_compaction_finish();
  FLOG_INFO("[TEST] finish ss minor compaction");
  int64_t macro_in_macro_cache_cnt = 0;
  check_macro_exist_in_macro_cache(run_ctx.tablet_id_.id(), macro_in_macro_cache_cnt);
  ASSERT_GT(macro_in_macro_cache_cnt, 0);
  int64_t should_in_macro_cache_cnt = 0;
  int64_t skipped_macro_cnt = 0;
  get_macro_cnt_in_local_cache(run_ctx.tablet_id_.id(), false/*is_hot_tablet*/, should_in_macro_cache_cnt, skipped_macro_cnt);
  ASSERT_GT(should_in_macro_cache_cnt, 0);
  ASSERT_EQ(should_in_macro_cache_cnt, macro_in_macro_cache_cnt);
  ObArray<blocksstable::MacroBlockId> block_ids;
  get_shared_blocks_for_tablet(block_ids);
  for (int64_t i = 0; (i < block_ids.count()); ++i) {
    if (block_ids.at(i).is_shared_data_or_meta()) {
      check_local_cache(block_ids.at(i), true/*expect_exist*/, false/*is_hot_tablet*/);
    }
  }
  FLOG_INFO("[TEST] finish test_minor_compact_prewarm", K(run_ctx), K(macro_in_macro_cache_cnt),
      K(should_in_macro_cache_cnt), K(skipped_macro_cnt));
}


TEST_F(ObPrereadMetaMacroTest, test_storage_cache_policy_prewarm)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx.tenant_id_));
  exe_prepare_sql();
  FLOG_INFO("[TEST] start test_storage_cache_policy_prewarm", K(run_ctx));
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  preread_task.is_inited_ = false;
  ObStorageCachePolicyPrewarmer prewarmer;
  
  OK(exe_sql("create table test_storage_cache_policy_prewarm (a bigint)"));
  set_ls_and_tablet_id_for_run_ctx("test_storage_cache_policy_prewarm");

  OK(exe_sql("insert into test_storage_cache_policy_prewarm select random(100) from table(generator(262144));"));
  sleep(1);
  OK(TestCompactionUtil::medium_compact(run_ctx.tenant_id_, run_ctx.tablet_id_.id(), run_ctx.ls_id_));
  FLOG_INFO("[TEST] finish medium compact (test_storage_cache_policy_prewarm)", K(run_ctx.tablet_id_));

  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObArray<blocksstable::MacroBlockId> block_ids;
  get_shared_blocks_for_tablet(block_ids);
  ASSERT_GT(block_ids.count(), 0);
  for (int64_t i = 0; i < block_ids.count(); ++i) {
    const MacroBlockId &macro_id = block_ids.at(i);
    if (macro_id.is_shared_data_or_meta()) {
      bool is_exist = true;
      OK(macro_cache_mgr->force_evict_by_macro_id(macro_id));
      OK(macro_cache_mgr->exist(macro_id, is_exist));
      ASSERT_FALSE(is_exist);
    }
  }

  // 1.test basic hot retention prewarm
  ObStorageCacheTabletTask *task = static_cast<ObStorageCacheTabletTask *>(ob_malloc(
      sizeof(ObStorageCacheTabletTask),
      ObMemAttr(run_ctx.tenant_id_, "ObStorageCache")));
  ASSERT_NE(nullptr, task);
  new (task) ObStorageCacheTabletTask();
  ObStorageCacheTabletTaskHandle task_handle;
  task_handle.set_ptr(task);
  PolicyStatus policy_status = PolicyStatus::HOT;
  OK(task->init_for_test(
      run_ctx.tenant_id_,
      run_ctx.ls_id_.id(),
      run_ctx.tablet_id_.id(),
      policy_status));
  OK(task->set_status(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING));

  // read init major
  OK(prewarmer.prewarm_tablet(task));
  const ObSSPrewarmStat &prewarm_stat = prewarmer.get_prewarm_stat();
  ASSERT_EQ(0, prewarm_stat.macro_block_fail_cnt_);
  ASSERT_GT(prewarm_stat.macro_block_num_, 0);
  ASSERT_EQ(prewarm_stat.macro_block_num_, prewarm_stat.macro_block_add_cnt_);
  ASSERT_EQ(prewarm_stat.get_total_macro_count(),
      prewarm_stat.get_macro_block_hit_cnt() + prewarm_stat.get_macro_block_add_cnt());
  int64_t macro_in_macro_cache_cnt = 0;
  check_macro_exist_in_macro_cache(run_ctx.tablet_id_.id(), macro_in_macro_cache_cnt);
  ASSERT_GT(macro_in_macro_cache_cnt, 0);
  int64_t should_in_macro_cache_cnt = 0;
  int64_t skipped_macro_cnt = 0;
  get_macro_cnt_in_local_cache(run_ctx.tablet_id_.id(), true/*is_hot_tablet*/, should_in_macro_cache_cnt, skipped_macro_cnt);
  ASSERT_GT(should_in_macro_cache_cnt, 0);
  ASSERT_EQ(should_in_macro_cache_cnt, macro_in_macro_cache_cnt);

  for (int64_t i = 0; (i < block_ids.count()); ++i) {
    if (block_ids.at(i).is_shared_data_or_meta()) {
      check_local_cache(block_ids.at(i), true/*expect_exist*/, true/*is_hot_tablet*/);
    }
  }

  // 2. 再次预热已全部命中的 tablet，确认 comment 能区分全命中和无宏块。
  LOG_INFO("[TEST] step 2: prewarm all-hit tablet", K(run_ctx.tablet_id_));
  ObStorageCachePolicyPrewarmer hit_prewarmer;
  OK(hit_prewarmer.prewarm_tablet(task));
  const ObSSPrewarmStat &hit_stat = hit_prewarmer.get_prewarm_stat();
  LOG_INFO("[TEST] step 2: verify all-hit prewarm stat", K(hit_stat));
  ASSERT_GT(hit_stat.get_total_macro_count(), 0);
  ASSERT_EQ(hit_stat.get_total_macro_count(), hit_stat.get_macro_block_hit_cnt());
  ASSERT_EQ(0, hit_stat.get_macro_block_add_cnt());
  ASSERT_EQ(0, hit_stat.get_macro_block_bytes());

  ObArenaAllocator comment_allocator(ObMemAttr(run_ctx.tenant_id_, "SCPTestComment"));
  ObString task_comment;
  ObSqlString expected_comment;
  OK(task->get_comment(comment_allocator, task_comment));
  OK(expected_comment.assign_fmt(
      "size=0, total_macro_count=%ld, macro_block_hit_count=%ld, macro_block_add_count=0.",
      hit_stat.get_total_macro_count(), hit_stat.get_macro_block_hit_cnt()));
  LOG_INFO("[TEST] verify all-hit prewarm comment", K(hit_stat), K(task_comment));
  ASSERT_STREQ(expected_comment.ptr(), task_comment.ptr());
  FLOG_INFO("[TEST] finish test_storage_cache_policy_prewarm", K(macro_in_macro_cache_cnt),
      K(should_in_macro_cache_cnt), K(skipped_macro_cnt));
}

// ----------------------------------------------------------------------------
// 单个 LS_MDS_TABLET_ID group task 只覆盖当前全部 LS 的 MDS tablet 实例
// TX_CTX、TX_DATA、LOCK、REORG_INFO 分别由各自 tablet_id 对应的 group task 处理。
//
// 场景：读取租户当前全部 LS，只统计每个 LS 中 MDS tablet 的 macro 数量。
// 操作：创建一个以 LS_MDS_TABLET_ID 为 key、未设置 ls_id 的 HOT partition task，
//       通过 process() 执行完整状态机和 do_prewarm()。
// 校验：task 完成后仍保持 group 身份；处理量恰好等于全部 LS 的 MDS macro 总量，
//       不包含其他 inner tablet。
// ----------------------------------------------------------------------------
TEST_F(ObPrereadMetaMacroTest, test_ls_inner_tablet_group_prewarm)
{
  LOG_INFO("========== test_ls_inner_tablet_group_prewarm begin ==========");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx.tenant_id_));

  ObLSService *ls_service = MTL(ObLSService *);
  compaction::ObTenantTabletScheduler *tablet_scheduler =
      MTL(compaction::ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, ls_service);
  ASSERT_NE(nullptr, tablet_scheduler);
  ObSEArray<ObLSID, 8> ls_ids;
  ls_ids.set_attr(ObMemAttr(run_ctx.tenant_id_, "InnerTestLS"));
  OK(ls_service->get_ls_ids(ls_ids));
  ASSERT_GT(ls_ids.count(), 0);

  bool mini_merge_disabled = false;
  bool minor_merge_disabled = false;
  bool major_merge_suspended = false;
  DEFER({
    int ret = OB_SUCCESS;
    if (major_merge_suspended) {
      ret = sys_exe_sql("alter system resume merge tenant = tt1;");
      if (OB_SUCCESS != ret) {
        LOG_WARN("fail to resume tenant major merge", KR(ret));
      }
      tablet_scheduler->resume_major_merge();
    }
    if (minor_merge_disabled) {
      ret = sys_exe_sql(
          "alter system set_tp tp_name = ERRSIM_DISABLE_MINOR_MERGE, "
          "error_code = 0, occur = 1, frequency = 0;");
      if (OB_SUCCESS != ret) {
        LOG_WARN("fail to reset minor merge tracepoint", KR(ret));
      }
    }
    if (mini_merge_disabled) {
      ret = sys_exe_sql(
          "alter system set_tp tp_name = ERRSIM_DISABLE_MINI_MERGE, "
          "error_code = 0, occur = 1, frequency = 0;");
      if (OB_SUCCESS != ret) {
        LOG_WARN("fail to reset mini merge tracepoint", KR(ret));
      }
    }
    LOG_INFO("finish to restore compaction",
        K(mini_merge_disabled), K(minor_merge_disabled), K(major_merge_suspended));
  });

  // step 1：禁止新 compaction，并取消、等待已经存在的 compaction，保证两次枚举使用稳定数据。
  OK(sys_exe_sql(
      "alter system set_tp tp_name = ERRSIM_DISABLE_MINI_MERGE, "
      "error_code = 1, occur = 1, frequency = 1;"));
  mini_merge_disabled = true;
  OK(sys_exe_sql(
      "alter system set_tp tp_name = ERRSIM_DISABLE_MINOR_MERGE, "
      "error_code = 1, occur = 1, frequency = 1;"));
  minor_merge_disabled = true;
  OK(sys_exe_sql("alter system suspend merge tenant = tt1;"));
  major_merge_suspended = true;
  tablet_scheduler->stop_major_merge();
  OK(cancel_and_wait_compaction_(ls_ids));

  // step 2：汇总全部 LS 中 MDS tablet 的 macro 数量，作为 group 预热的期望值。
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 16> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx.tenant_id_, "InnerTestData"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx.tenant_id_, "InnerTestMeta"));

  const ObTabletID tablet_id(ObTabletID::LS_MDS_TABLET_ID);
  int64_t expected_macro_count = 0;
  for (int64_t i = 0; i < ls_ids.count(); ++i) {
    const ObLSID &ls_id = ls_ids.at(i);
    OK(get_macro_blocks(ls_id, tablet_id, data_block_ids, meta_block_ids));
    const int64_t instance_macro_count = data_block_ids.count() + meta_block_ids.count();
    expected_macro_count += instance_macro_count;
    LOG_INFO("step 2: collect ls inner tablet macros",
        K(i), K(ls_id), K(tablet_id), K(instance_macro_count), K(expected_macro_count));
  }
  ASSERT_GT(expected_macro_count, 0);

  // step 3：不设置 ls_id，执行以 LS_MDS_TABLET_ID 为 key 的 group task。
  ObStorageCacheTabletTask task;
  OK(task.init_partition_task(run_ctx.tenant_id_, tablet_id.id(), PolicyStatus::HOT));
  ASSERT_EQ(static_cast<int64_t>(ObLSID::INVALID_LS_ID), task.get_ls_id());
  OK(task.process());

  // step 4：cache hit 和实际预热数量之和必须恰好覆盖全部 LS 的 MDS macro。
  // 若错误地预热了其他 inner tablet，processed_macro_count 将大于 expected_macro_count。
  const ObSSPrewarmStat prewarm_stat = task.get_prewarm_stat();
  const int64_t processed_macro_count =
      prewarm_stat.macro_block_num_ + prewarm_stat.macro_block_hit_cnt_;
  LOG_INFO("step 4: verify ls inner tablet group prewarm",
      K(expected_macro_count), K(processed_macro_count), K(prewarm_stat), K(ls_ids), K(task));
  ASSERT_EQ(expected_macro_count, prewarm_stat.get_total_macro_count());
  ASSERT_EQ(expected_macro_count, processed_macro_count);
  ASSERT_EQ(0, prewarm_stat.macro_block_fail_cnt_);
  ASSERT_EQ(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED, task.get_status());
  ASSERT_EQ(static_cast<int64_t>(ObLSID::INVALID_LS_ID), task.get_ls_id());
  LOG_INFO("========== test_ls_inner_tablet_group_prewarm end ==========");
}

// Disabled: depends on ObTabletSSMiniMergeCtx hot/cold macro cache behavior removed in inc-upload refactor.
// TODO (jyx441808): decide if we need to keep this test
TEST_F(ObPrereadMetaMacroTest, DISABLED_test_major_compaction_prewarm)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx.tenant_id_));
  OK(sys_exe_sql("alter system set _enable_adaptive_compaction = false tenant tt1;"));
  OK(sys_exe_sql("alter system set _major_compaction_prewarm_level = 0 tenant tt1;"));
  exe_prepare_sql();
  FLOG_INFO("[TEST] start test_major_compaction_prewarm", K(run_ctx));
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  preread_task.is_inited_ = false;
  
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  
  OK(exe_sql("create table test_major_compaction_prewarm (a bigint) storage_cache_policy = (global = 'hot')"));
  set_ls_and_tablet_id_for_run_ctx("test_major_compaction_prewarm");
  OK(exe_sql("insert into test_major_compaction_prewarm select random(100) from table(generator(262144))"));
  sleep(1);
  OK(TestCompactionUtil::medium_compact(run_ctx.tenant_id_, run_ctx.tablet_id_.id(), run_ctx.ls_id_));
  FLOG_INFO("[TEST] finish medium compact");

  int64_t macro_in_macro_cache_cnt = 0;
  check_macro_exist_in_macro_cache(run_ctx.tablet_id_.id(), macro_in_macro_cache_cnt);
  ASSERT_GT(macro_in_macro_cache_cnt, 0);
  int64_t should_in_macro_cache_cnt = 0;
  int64_t skipped_macro_cnt = 0;
  get_macro_cnt_in_local_cache(run_ctx.tablet_id_.id(), true/*is_hot_tablet*/, should_in_macro_cache_cnt, skipped_macro_cnt);
  ASSERT_GT(should_in_macro_cache_cnt, 0);
  ASSERT_EQ(should_in_macro_cache_cnt, macro_in_macro_cache_cnt);

  ObArray<blocksstable::MacroBlockId> block_ids;
  get_shared_blocks_for_tablet(block_ids);
  for (int64_t i = 0; (i < block_ids.count()); ++i) {
    if (block_ids.at(i).is_shared_data_or_meta()) {
      check_local_cache(block_ids.at(i), true/*expect_exist*/, true/*is_hot_tablet*/);
    }
  }
  FLOG_INFO("[TEST] finish test_major_compaction_prewarm", K(run_ctx), K(macro_in_macro_cache_cnt),
      K(should_in_macro_cache_cnt), K(skipped_macro_cnt));
}
TEST_F(ObPrereadMetaMacroTest, end)
{
  if (run_ctx.time_sec_ > 0) {
    ::sleep(run_ctx.time_sec_);
  }
}
} // end unittest
} // end oceanbase


int main(int argc, char **argv)
{
  int64_t c = 0;
  int64_t time_sec = 0;
  char *log_level = (char*)"INFO";
  char buf[1000];
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  memset(buf, 1000, sizeof(buf));
  databuff_printf(buf, sizeof(buf), "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region", 
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT, oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  while(EOF != (c = getopt(argc,argv,"t:l:"))) {
    switch(c) {
    case 't':
      time_sec = atoi(optarg);
      break;
    case 'l':
     log_level = optarg;
     oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
     break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");

  LOG_INFO("main>>>");
  oceanbase::unittest::run_ctx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
