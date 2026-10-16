/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <algorithm>
#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#define UNITTEST

#include "lib/guard/ob_scope_guard.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "src/storage/incremental/ob_inc_replace_upload_task.h"
#include "src/storage/incremental/ob_inc_ss_dag_util.h"
#include "src/storage/incremental/ob_shared_meta_service.h"
#include "src/storage/shared_storage/ob_private_block_gc_task.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "src/storage/shared_storage/ob_dir_manager.h"
#include "share/ob_io_device_helper.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/init_basic_struct.h"
#include "rootserver/ob_ls_recovery_reportor.h"
#include "storage/shared_storage/ob_file_op.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace sslog
{


} // namespace sslog
} // namespace oceanbase


namespace oceanbase
{
char *shared_storage_info = NULL;
int64_t tx_data_recycle_scn = 0;

namespace storage
{
#ifdef OB_BUILD_SHARED_STORAGE
int ObTxTable::resolve_shared_storage_upload_info_(share::SCN &tablet_recycle_scn)
{
  int ret = OB_SUCCESS;
  const int64_t SHARED_STORAGE_USE_CACHE_DURATION = 1_hour;

  SCN tx_data_table_upload_scn = SCN::invalid_scn();
  SCN data_upload_min_end_scn = SCN::invalid_scn();
  int64_t origin_time = ATOMIC_LOAD(&(ss_upload_scn_cache_.update_ts_));
  int64_t current_time = ObClockGenerator::getClock();

  if (current_time - origin_time > 1_s
      && ATOMIC_BCAS(&(ss_upload_scn_cache_.update_ts_), origin_time, current_time)) {
    share::SCN transfer_scn;
    {
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(LS_TX_DATA_TABLET, tablet_handle))) {
        TRANS_LOG(WARN, "get tx data tablet fail", K(ret));
      } else {
        transfer_scn = tablet_handle.get_obj()->get_reorganization_scn();
      }
    }
    // Tip1: may output min_scn if no uploads exists or max_uploaded_scn if
    // there exists
    if (FAILEDx(ls_->get_inc_sstable_uploader().
                get_tablet_upload_pos(LS_TX_DATA_TABLET,
                                      transfer_scn,
                                      tx_data_table_upload_scn))) {
      TRANS_LOG(WARN, "get tablet upload pos failed", K(ret));
    // Tip2: may output min_scn if no uploads exists or max_uploaded_scn if
    // there exists
    } else if (OB_FAIL(ls_->get_inc_sstable_uploader().
                       get_upload_min_end_scn_from_ss(data_upload_min_end_scn))) {
      TRANS_LOG(WARN, "get tablet upload min end scn failed", K(ret));
    // We need ensure that no concurrent user after an hour later will
    // concurrently change the cache
    } else if (ATOMIC_BCAS(&(ss_upload_scn_cache_.update_ts_),
                           current_time, current_time + 1)) {
      ss_upload_scn_cache_.tx_table_upload_max_scn_cache_.atomic_store(tx_data_table_upload_scn);
      ss_upload_scn_cache_.data_upload_min_end_scn_cache_.atomic_store(data_upload_min_end_scn);
      TRANS_LOG(INFO, "ss_upload_scn updated successfully", K(ss_upload_scn_cache_));
    }

    if (OB_FAIL(ret)) {
      if (!ATOMIC_BCAS(&(ss_upload_scn_cache_.update_ts_), current_time, origin_time)) {
        TRANS_LOG(WARN, "ss_upload_scn_cache is updated by a concurrent user after an hour!!!",
                  K(ss_upload_scn_cache_));
      }
      // under error during get upload info, we choose to use cache instead
      ret = OB_SUCCESS;
    }
  }


  if (OB_SUCC(ret)) {
    tx_data_table_upload_scn = ss_upload_scn_cache_.tx_table_upload_max_scn_cache_.atomic_load();
    data_upload_min_end_scn = ss_upload_scn_cache_.data_upload_min_end_scn_cache_.atomic_load();

    // Tip1: we need to obtain the upload location for the tx_data_table to
    // ensure that transaction data that hasn't been uploaded is not recycled,
    // thereby maintaining the integrity of transaction data on shared storage.
    // It is important to note that this is not a correctness requirement but
    // rather for facilitating better troubleshooting in the future.
    if (!tx_data_table_upload_scn.is_valid()) {
      // we havenot aleady upload anything in cache
      tablet_recycle_scn.set_min();
    } else if (tx_data_table_upload_scn < tablet_recycle_scn) {
      tablet_recycle_scn = tx_data_table_upload_scn;
    }

    // Tip2: we need to obtain the smallest end_scn among all tablets that have
    // uploaded its sstables. This ensures that uncommitted data, which has not
    // been backfilled, can certainly be interpreted by the transaction data on
    // shared storage. It is important to note that this is a correctness
    // requirement. By also ensuring that computing nodes do not reclaim
    // transaction data, we can guarantee that any data retrieved from shared
    // storage can be interpreted properly.
    if (!data_upload_min_end_scn.is_valid()) {
      // we havenot aleady upload anything in cache
      tablet_recycle_scn.set_min();
    } else if (data_upload_min_end_scn < tablet_recycle_scn) {
      tablet_recycle_scn = data_upload_min_end_scn;
    }

    if (1002 == MTL_ID() && ls_id_.id() > 1000) {
      tx_data_recycle_scn = tablet_recycle_scn.get_val_for_tx();
      TRANS_LOG(INFO, "qianchen debug", K(tx_data_recycle_scn));
    }
  }

  return ret;
}
#endif
}

namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using oceanbase::common::ScopeGuard;


class TestRunCtx
{
public:
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;
  int64_t time_sec_ = 0;
};

TestRunCtx RunCtx;

struct TxDataChainInfo
{
  TxDataChainInfo() { reset(); }

  void reset()
  {
    mini_cnt_ = 0;
    minor_cnt_ = 0;
    total_row_cnt_ = 0;
    first_table_type_ = ObITable::TableType::MAX_TABLE_TYPE;
    first_row_cnt_ = 0;
    first_start_scn_.set_invalid();
    first_end_scn_.set_invalid();
    first_filled_tx_scn_.set_invalid();
    first_minor_start_scn_.set_invalid();
    first_minor_filled_tx_scn_.set_invalid();
    last_end_scn_.set_invalid();
  }

  TO_STRING_KV(K_(mini_cnt),
               K_(minor_cnt),
               K_(total_row_cnt),
               K_(first_table_type),
               K_(first_row_cnt),
               K_(first_start_scn),
               K_(first_end_scn),
               K_(first_filled_tx_scn),
               K_(first_minor_start_scn),
               K_(first_minor_filled_tx_scn),
               K_(last_end_scn));

  int64_t mini_cnt_;
  int64_t minor_cnt_;
  int64_t total_row_cnt_;
  ObITable::TableType first_table_type_;
  int64_t first_row_cnt_;
  share::SCN first_start_scn_;
  share::SCN first_end_scn_;
  share::SCN first_filled_tx_scn_;
  share::SCN first_minor_start_scn_;
  share::SCN first_minor_filled_tx_scn_;
  share::SCN last_end_scn_;
};

class ObSharedStorageTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObSharedStorageTest() : ObSimpleClusterTestBase("test_shared_storage_tx_data_gc_", "50G", "50G", "50G")
  {}
  void wait_minor_finish();
  void set_ls_and_tablet_id_for_run_ctx(const char *table_name = "test_table");
  void get_row_scn(const char *value, int64_t &row_scn);
  int get_shared_tx_data_tablet(ObArenaAllocator &allocator, ObTabletHandle &tablet_handle);
  int get_tx_data_chain_info(ObTablet &tablet, TxDataChainInfo &info);
  int get_shared_tx_data_chain_info(TxDataChainInfo &info);
  int get_local_tx_data_chain_info(ObLS &ls, TxDataChainInfo &info);
  int get_local_tx_data_recycle_candidate(ObLS &ls, share::SCN &recycle_scn);
  int execute_sys_sql(const char *sql_str);
  int execute_tenant_sql(const char *sql_str);
  int block_inc_upload_before_schedule();
  int resume_inc_upload();
  int cleanup_tx_data_replace_test();
  void write_tx_rows(const char *table_name, const int64_t start_value, const int64_t row_cnt);
  void freeze_and_wait_tx_data_mini_uploaded();

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }
};

TEST_F(ObSharedStorageTest, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

#define EXE_SQL(sql_str)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows));

TEST_F(ObSharedStorageTest, add_tenant)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant("tt1", "5G", "10G", false, 10));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

int ObSharedStorageTest::get_shared_tx_data_tablet(
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *ss_meta_service = MTL(ObSSMetaService *);
  ObSSMetaReadParamV2 read_param;
  read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_PREFIX,
                                    ObSSMetaReadResultType::READ_WHOLE_ROW,
                                    ObAtomicProtocolTypeEnum::TABLET_META,
                                    RunCtx.ls_id_,
                                    LS_TX_DATA_TABLET,
                                    share::SCN::min_scn() /*meta_scn*/,
                                    share::SCN::min_scn() /*reorganization_scn*/,
                                    true /*is_in_table*/);
  if (OB_ISNULL(ss_meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss meta service is null", KR(ret), K(RunCtx.ls_id_));
  } else if (OB_FAIL(ss_meta_service->get_tablet(
                 read_param, share::SCN::max_scn(), allocator, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("get shared tx data tablet failed", KR(ret), K(RunCtx.ls_id_));
    }
  }
  return ret;
}

int ObSharedStorageTest::get_tx_data_chain_info(ObTablet &tablet, TxDataChainInfo &info)
{
  int ret = OB_SUCCESS;
  info.reset();
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTableStoreIterator table_store_iter;
  if (OB_FAIL(tablet.fetch_table_store_with_prewarm(table_store_wrapper))) {
    LOG_WARN("fetch tx data table store failed", KR(ret), K(tablet));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_all_minor_sstables(table_store_iter))) {
    LOG_WARN("get tx data minor sstables failed", KR(ret), K(tablet));
  } else {
    share::SCN first_minor_end_scn;
    first_minor_end_scn.set_invalid();
    while (OB_SUCC(ret)) {
      ObTableHandleV2 table_handle;
      blocksstable::ObSSTable *sstable = nullptr;
      blocksstable::ObSSTableMetaHandle sst_meta_hdl;
      if (OB_FAIL(table_store_iter.get_next(table_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next tx data sstable failed", KR(ret), K(tablet));
        }
      } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
        LOG_WARN("get tx data sstable failed", KR(ret), K(table_handle));
      } else if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tx data sstable is null", KR(ret), K(table_handle));
      } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
        LOG_WARN("get tx data sstable meta failed", KR(ret), KPC(sstable));
      } else {
        const ObITable::TableType table_type = sstable->get_key().table_type_;
        const share::SCN filled_tx_scn = sst_meta_hdl.get_sstable_meta().get_filled_tx_scn();
        const share::SCN end_scn = sstable->get_end_scn();
        info.total_row_cnt_ += sstable->get_row_count();
        if (!info.first_end_scn_.is_valid() || end_scn < info.first_end_scn_) {
          info.first_table_type_ = table_type;
          info.first_row_cnt_ = sstable->get_row_count();
          info.first_start_scn_ = sstable->get_start_scn();
          info.first_end_scn_ = end_scn;
          info.first_filled_tx_scn_ = filled_tx_scn;
        }
        if (!info.last_end_scn_.is_valid() || info.last_end_scn_ < end_scn) {
          info.last_end_scn_ = end_scn;
        }
        if (ObITable::TableType::MINI_SSTABLE == table_type) {
          ++info.mini_cnt_;
        } else if (ObITable::TableType::MINOR_SSTABLE == table_type) {
          if (!first_minor_end_scn.is_valid() || end_scn < first_minor_end_scn) {
            first_minor_end_scn = end_scn;
            info.first_minor_start_scn_ = sstable->get_start_scn();
            info.first_minor_filled_tx_scn_ = filled_tx_scn;
          }
          ++info.minor_cnt_;
        }
      }
    }
  }
  return ret;
}

int ObSharedStorageTest::get_shared_tx_data_chain_info(TxDataChainInfo &info)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "GetSSTxChain"));
  ObTabletHandle tablet_handle;
  if (OB_FAIL(get_shared_tx_data_tablet(allocator, tablet_handle))) {
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared tx data tablet is null", KR(ret), K(RunCtx.ls_id_));
  } else if (OB_FAIL(get_tx_data_chain_info(*tablet_handle.get_obj(), info))) {
    LOG_WARN("get shared tx data chain info failed", KR(ret), K(RunCtx.ls_id_));
  }
  LOG_INFO("get shared tx data chain info", KR(ret), K(RunCtx.ls_id_), K(info));
  return ret;
}

int ObSharedStorageTest::get_local_tx_data_chain_info(ObLS &ls, TxDataChainInfo &info)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  if (OB_FAIL(ls.get_tablet_svr()->get_tablet(
          LS_TX_DATA_TABLET,
          tablet_handle,
          ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S,
          ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get local tx data tablet failed", KR(ret), K(RunCtx.ls_id_));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local tx data tablet is null", KR(ret), K(RunCtx.ls_id_));
  } else if (OB_FAIL(get_tx_data_chain_info(*tablet_handle.get_obj(), info))) {
    LOG_WARN("get local tx data chain info failed", KR(ret), K(RunCtx.ls_id_));
  }
  LOG_INFO("get local tx data chain info", KR(ret), K(RunCtx.ls_id_), K(info));
  return ret;
}

int ObSharedStorageTest::get_local_tx_data_recycle_candidate(
    ObLS &ls,
    share::SCN &recycle_scn)
{
  int ret = OB_SUCCESS;
  share::SCN min_end_scn_from_latest_tablets = share::SCN::max_scn();
  share::SCN min_end_scn_from_old_tablets = share::SCN::max_scn();
  share::SCN min_end_scn_from_major_dag = share::SCN::max_scn();
  recycle_scn.set_min();
  if (OB_FAIL(ls.get_tablet_svr()->get_ls_min_end_scn(
          min_end_scn_from_latest_tablets, min_end_scn_from_old_tablets))) {
    LOG_WARN("get local tx data recycle candidate from tablets failed", KR(ret), K(ls));
  } else if (OB_FAIL(MTL(share::ObTenantDagScheduler *)->get_min_end_scn_from_major_dag(
                 ls.get_ls_id(), min_end_scn_from_major_dag))) {
    LOG_WARN("get local tx data recycle candidate from major dag failed", KR(ret), K(ls));
  } else {
    recycle_scn = std::min(min_end_scn_from_latest_tablets, min_end_scn_from_old_tablets);
    recycle_scn = std::min(recycle_scn, min_end_scn_from_major_dag);
    if (recycle_scn.is_max()) {
      recycle_scn.set_min();
    } else {
      share::SCN snapshot_version;
      MonotonicTs unused_ts(0);
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(OB_TS_MGR.get_gts(MTL_ID(), MonotonicTs(1), nullptr,
                                        snapshot_version, unused_ts))) {
        recycle_scn.set_min();
        LOG_WARN("get gts for local tx data recycle candidate failed", K(tmp_ret), K(ls));
      } else {
        recycle_scn = std::min(recycle_scn, snapshot_version);
      }
    }
  }
  return ret;
}

int ObSharedStorageTest::execute_sys_sql(const char *sql_str)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_FAIL(sql.assign(sql_str))) {
    LOG_WARN("assign sys sql failed", KR(ret), K(sql_str));
  } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy().write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sys sql failed", KR(ret), K(sql));
  }
  return ret;
}

int ObSharedStorageTest::execute_tenant_sql(const char *sql_str)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_FAIL(sql.assign(sql_str))) {
    LOG_WARN("assign tenant sql failed", KR(ret), K(sql_str));
  } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute tenant sql failed", KR(ret), K(sql));
  }
  return ret;
}

int ObSharedStorageTest::block_inc_upload_before_schedule()
{
  int ret = OB_SUCCESS;
  const int64_t debug_sync_timeout_us = 300 * 1000 * 1000L;
  const int64_t config_wait_timeout_us = 10 * 1000 * 1000L;
  if (OB_FAIL(execute_sys_sql("alter system set debug_sync_timeout = '300s'"))) {
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    while (debug_sync_timeout_us != static_cast<int64_t>(GCONF.debug_sync_timeout)
        && ObTimeUtility::current_time() - start_ts < config_wait_timeout_us) {
      ob_usleep(100 * 1000);
    }
    if (debug_sync_timeout_us != static_cast<int64_t>(GCONF.debug_sync_timeout)) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait debug sync config timeout", KR(ret), K(debug_sync_timeout_us),
               "actual_timeout_us", static_cast<int64_t>(GCONF.debug_sync_timeout));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(execute_sys_sql(
          "set ob_global_debug_sync = 'BEFORE_SS_SCHEDULE_INC_UPLOAD "
          "wait_for tx_data_replace_upload execute 10000'"))) {
  }
  return ret;
}

int ObSharedStorageTest::resume_inc_upload()
{
  int ret = OB_SUCCESS;
  int tmp_ret = execute_sys_sql(
      "set ob_global_debug_sync = 'BEFORE_SS_SCHEDULE_INC_UPLOAD clear'");
  if (OB_SUCCESS != tmp_ret) {
    ret = tmp_ret;
  }
  tmp_ret = execute_sys_sql(
      "set ob_global_debug_sync = 'now broadcast tx_data_replace_upload'");
  if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
    ret = tmp_ret;
  }
  tmp_ret = execute_sys_sql("alter system set debug_sync_timeout = '0'");
  if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
    ret = tmp_ret;
  }
  return ret;
}

int ObSharedStorageTest::cleanup_tx_data_replace_test()
{
  int ret = OB_SUCCESS;
  const char *cleanup_sqls[] = {
      "alter system set minor_compact_trigger = 2",
      "alter system set _minor_merge_write_amplification_threshold = 2000000",
      "alter system set _minor_compaction_amplification_factor = 0",
      "alter system set _shared_inc_sstable_upload_interval = '1s'",
      "alter system set ob_compaction_schedule_interval = '3s'",
      "drop table if exists test_tx_data_watermark"};
  for (int64_t i = 0; i < ARRAYSIZEOF(cleanup_sqls); ++i) {
    const int tmp_ret = execute_tenant_sql(cleanup_sqls[i]);
    if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
      ret = tmp_ret;
    }
  }
  return ret;
}

void ObSharedStorageTest::write_tx_rows(
    const char *table_name,
    const int64_t start_value,
    const int64_t row_cnt)
{
  ObSqlString sql;
  int64_t affected_rows = 0;
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "insert into %s values (%ld)", table_name, start_value + i));
    ASSERT_EQ(OB_SUCCESS,
              get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows));
    sql.reset();
  }
}

void ObSharedStorageTest::freeze_and_wait_tx_data_mini_uploaded()
{
  int ret = OB_SUCCESS;
  TxDataChainInfo before_info;
  TxDataChainInfo current_info;
  ret = get_shared_tx_data_chain_info(before_info);
  if (OB_TABLET_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    before_info.reset();
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, before_info.minor_cnt_);
  const int64_t before_table_cnt = before_info.mini_cnt_ + before_info.minor_cnt_;

  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system minor freeze tenant tt1"));
  ASSERT_EQ(OB_SUCCESS,
            get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows));

  const int64_t start_ts = ObTimeUtility::current_time();
  const int64_t timeout_us = 120 * 1000 * 1000L;
  do {
    ret = get_shared_tx_data_chain_info(current_info);
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      current_info.reset();
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, current_info.minor_cnt_);
    if (current_info.mini_cnt_ + current_info.minor_cnt_ <= before_table_cnt) {
      ob_usleep(100 * 1000);
      ASSERT_LT(ObTimeUtility::current_time() - start_ts, timeout_us);
    }
  } while (current_info.mini_cnt_ + current_info.minor_cnt_ <= before_table_cnt);
}

TEST_F(ObSharedStorageTest, leading_mini_is_preserved_by_replace_upload)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  ObSqlString sql;
  int64_t affected_rows = 0;
  bool need_cleanup_test = true;
  auto test_cleanup_guard = MAKE_SCOPE(need_cleanup_test, [this](bool &need_cleanup) {
    if (need_cleanup) {
      EXPECT_EQ(OB_SUCCESS, cleanup_tx_data_replace_test());
    }
  });
  EXE_SQL("alter system set _shared_inc_sstable_upload_interval = '1s'");
  EXE_SQL("alter system set inc_sstable_upload_thread_score = 20");
  // 创建 SSTable 期间关闭后台 compaction，后续由测试显式调度 Shared minor。
  EXE_SQL("alter system set ob_compaction_schedule_interval = '5m'");
  EXE_SQL("alter system set minor_compact_trigger = 16");
  EXE_SQL("alter system set _minor_merge_write_amplification_threshold = 32");
  EXE_SQL("alter system set _minor_compaction_amplification_factor = 100");
  EXE_SQL("create table test_tx_data_watermark (a int primary key)");
  set_ls_and_tablet_id_for_run_ctx("test_tx_data_watermark");

  TxDataChainInfo initial_info;
  int ret = get_shared_tx_data_chain_info(initial_info);
  if (OB_TABLET_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    initial_info.reset();
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, initial_info.mini_cnt_ + initial_info.minor_cnt_);

  // 64 个独立事务形成链首大 MINI，后续 3 个单事务 MINI 构成可被 refine 的尾部。
  write_tx_rows("test_tx_data_watermark", 0, 64);
  freeze_and_wait_tx_data_mini_uploaded();
  for (int64_t i = 0; i < 3; ++i) {
    write_tx_rows("test_tx_data_watermark", 100 + i, 1);
    freeze_and_wait_tx_data_mini_uploaded();
  }

  TxDataChainInfo all_mini_info;
  ASSERT_EQ(OB_SUCCESS, get_shared_tx_data_chain_info(all_mini_info));
  ASSERT_GE(all_mini_info.mini_cnt_, 4);
  ASSERT_EQ(0, all_mini_info.minor_cnt_);
  ASSERT_EQ(ObITable::TableType::MINI_SSTABLE, all_mini_info.first_table_type_);
  ASSERT_GT(all_mini_info.first_row_cnt_, 32);

  EXE_SQL("alter system set minor_compact_trigger = 2");
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS,
            MTL(ObLSService *)->get_ls(RunCtx.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSTxRefine"));
  ObTabletHandle shared_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_shared_tx_data_tablet(allocator, shared_tablet_handle));
  ASSERT_TRUE(shared_tablet_handle.is_valid());
  ObGetMergeTablesResult merge_result;
  ASSERT_EQ(OB_SUCCESS,
            compaction::ObPartitionMergePolicy::get_ss_minor_merge_tables(
                *ls, *shared_tablet_handle.get_obj(), merge_result));
  ASSERT_GT(merge_result.handle_.get_count(), 2);
  ASSERT_FALSE(merge_result.scn_range_.start_scn_.is_base_scn());
  ASSERT_EQ(all_mini_info.first_end_scn_, merge_result.scn_range_.start_scn_);

  ObTabletHandle local_tx_data_tablet_handle;
  ASSERT_EQ(OB_SUCCESS,
            ls->get_tablet_svr()->get_tablet(
                LS_TX_DATA_TABLET,
                local_tx_data_tablet_handle,
                ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S,
                ObMDSGetTabletMode::READ_WITHOUT_CHECK));
  ASSERT_EQ(OB_SUCCESS,
            compaction::ObTenantTabletScheduler::schedule_tablet_ss_minor_merge(
                ls_handle,
                LS_TX_DATA_TABLET,
                local_tx_data_tablet_handle,
                share::SCN::min_scn()));

  TxDataChainInfo mixed_chain_info;
  const int64_t start_ts = ObTimeUtility::current_time();
  const int64_t timeout_us = 120 * 1000 * 1000L;
  do {
    ASSERT_EQ(OB_SUCCESS, get_shared_tx_data_chain_info(mixed_chain_info));
    if (mixed_chain_info.mini_cnt_ <= 0
        || mixed_chain_info.minor_cnt_ <= 0
        || ObITable::TableType::MINI_SSTABLE != mixed_chain_info.first_table_type_) {
      ob_usleep(100 * 1000);
      ASSERT_LT(ObTimeUtility::current_time() - start_ts, timeout_us);
    }
  } while (mixed_chain_info.mini_cnt_ <= 0
      || mixed_chain_info.minor_cnt_ <= 0
      || ObITable::TableType::MINI_SSTABLE != mixed_chain_info.first_table_type_);
  ASSERT_TRUE(mixed_chain_info.first_minor_filled_tx_scn_.is_valid());
  ASSERT_FALSE(mixed_chain_info.first_minor_filled_tx_scn_.is_min());
  ASSERT_FALSE(mixed_chain_info.first_minor_filled_tx_scn_.is_max());
  ASSERT_FALSE(mixed_chain_info.first_minor_start_scn_.is_base_scn());

  EXE_SQL("alter system set _tx_result_retention = 0");
  // 先排除本地位点尚未推进这一假通过：不考虑 Shared 约束时，回收位点已越过尾部 MINOR 水位。
  share::SCN local_recycle_candidate;
  const int64_t recycle_wait_start_ts = ObTimeUtility::current_time();
  do {
    ASSERT_EQ(OB_SUCCESS, get_local_tx_data_recycle_candidate(*ls, local_recycle_candidate));
    if (!local_recycle_candidate.is_valid_and_not_min()
        || local_recycle_candidate < mixed_chain_info.first_minor_filled_tx_scn_) {
      ob_usleep(100 * 1000);
      ASSERT_LT(ObTimeUtility::current_time() - recycle_wait_start_ts, timeout_us);
    }
  } while (!local_recycle_candidate.is_valid_and_not_min()
      || local_recycle_candidate < mixed_chain_info.first_minor_filled_tx_scn_);

  ObTxTableGuard tx_table_guard;
  ASSERT_EQ(OB_SUCCESS, ls->get_tx_table_guard(tx_table_guard));
  share::SCN recycle_scn;
  ASSERT_EQ(OB_SUCCESS, tx_table_guard.get_recycle_scn(recycle_scn));
  // 链首仍为 MINI，真实回收入口必须禁止本轮本地 TX_DATA 回收。
  EXPECT_TRUE(recycle_scn.is_min());

  TxDataChainInfo local_before_suffix_info;
  ASSERT_EQ(OB_SUCCESS, get_local_tx_data_chain_info(*ls, local_before_suffix_info));
  ASSERT_TRUE(local_before_suffix_info.last_end_scn_.is_valid_and_not_min());

  // 保留一个尚未上传的本地后缀，确保 replace 结果的右边界能越过当前 Shared 链。
  EXE_SQL("alter system set _shared_inc_sstable_upload_interval = '5m'");
  bool need_resume_inc_upload = true;
  auto inc_upload_guard = MAKE_SCOPE(need_resume_inc_upload, [this](bool &need_resume) {
    if (need_resume) {
      EXPECT_EQ(OB_SUCCESS, resume_inc_upload());
    }
  });
  ASSERT_EQ(OB_SUCCESS, block_inc_upload_before_schedule());
  write_tx_rows("test_tx_data_watermark", 1000, 1);
  EXE_SQL("alter system minor freeze tenant tt1");

  TxDataChainInfo local_before_replace_info;
  const int64_t local_wait_start_ts = ObTimeUtility::current_time();
  do {
    ASSERT_EQ(OB_SUCCESS, get_local_tx_data_chain_info(*ls, local_before_replace_info));
    if (!local_before_replace_info.last_end_scn_.is_valid()
        || local_before_replace_info.last_end_scn_ <= local_before_suffix_info.last_end_scn_) {
      ob_usleep(100 * 1000);
      ASSERT_LT(ObTimeUtility::current_time() - local_wait_start_ts, timeout_us);
    }
  } while (!local_before_replace_info.last_end_scn_.is_valid()
      || local_before_replace_info.last_end_scn_ <= local_before_suffix_info.last_end_scn_);

  TxDataChainInfo shared_before_replace_info;
  ASSERT_EQ(OB_SUCCESS, get_shared_tx_data_chain_info(shared_before_replace_info));
  ASSERT_EQ(mixed_chain_info.last_end_scn_, shared_before_replace_info.last_end_scn_);
  ASSERT_EQ(mixed_chain_info.total_row_cnt_, shared_before_replace_info.total_row_cnt_);
  // replace 覆盖本地 base 起始表，prepare_tx_table_compaction_filter_ 会真实进入过滤器初始化。
  ASSERT_TRUE(local_before_replace_info.first_start_scn_.is_base_scn());
  ASSERT_TRUE(local_before_replace_info.first_filled_tx_scn_.is_valid_and_not_min());
  ASSERT_LT(local_before_replace_info.first_filled_tx_scn_,
            mixed_chain_info.first_minor_filled_tx_scn_);
  ASSERT_GT(local_before_replace_info.last_end_scn_, shared_before_replace_info.last_end_scn_);
  ASSERT_GT(local_before_replace_info.total_row_cnt_, shared_before_replace_info.total_row_cnt_);

  ObTabletHandle replace_local_tablet_handle;
  ASSERT_EQ(OB_SUCCESS,
            ls->get_tablet_svr()->get_tablet(
                LS_TX_DATA_TABLET,
                replace_local_tablet_handle,
                ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S,
                ObMDSGetTabletMode::READ_WITHOUT_CHECK));
  ASSERT_TRUE(replace_local_tablet_handle.is_valid());
  ObIncReplaceUploadParam replace_param;
  ASSERT_EQ(OB_SUCCESS,
            replace_param.init(
                RunCtx.ls_id_,
                LS_TX_DATA_TABLET,
                replace_local_tablet_handle.get_obj()->get_reorganization_scn()));
  const int64_t tx_ready_wait_start_ts = ObTimeUtility::current_time();
  while (!compaction::ObTenantTabletScheduler::check_tx_table_ready(
      *ls, local_before_replace_info.last_end_scn_)) {
    ob_usleep(100 * 1000);
    ASSERT_LT(ObTimeUtility::current_time() - tx_ready_wait_start_ts, timeout_us);
  }
  ASSERT_EQ(OB_SUCCESS, ObIncSSDagUtil::schedule_replace_upload_dag(replace_param, true));

  TxDataChainInfo replaced_info;
  const int64_t replace_wait_start_ts = ObTimeUtility::current_time();
  do {
    ASSERT_EQ(OB_SUCCESS, get_shared_tx_data_chain_info(replaced_info));
    if (0 != replaced_info.mini_cnt_
        || 1 != replaced_info.minor_cnt_
        || replaced_info.last_end_scn_ != local_before_replace_info.last_end_scn_) {
      ob_usleep(100 * 1000);
      ASSERT_LT(ObTimeUtility::current_time() - replace_wait_start_ts, timeout_us);
    }
  } while (0 != replaced_info.mini_cnt_
      || 1 != replaced_info.minor_cnt_
      || replaced_info.last_end_scn_ != local_before_replace_info.last_end_scn_);

  LOG_INFO("verify tx data replace upload keeps the leading mini rows",
           K(all_mini_info), K(mixed_chain_info), K(local_recycle_candidate), K(recycle_scn),
           K(local_before_replace_info), K(replaced_info));
  ASSERT_EQ(ObITable::TableType::MINOR_SSTABLE, replaced_info.first_table_type_);
  // 普通 Minor Merge 可能合并多版本物理行，不能用行数相等判断是否回收；
  // 未启用过滤时，replace 产物必须继承最老输入表的 filled_tx_scn。
  ASSERT_EQ(local_before_replace_info.first_filled_tx_scn_,
            replaced_info.first_filled_tx_scn_);
  ASSERT_EQ(OB_SUCCESS, resume_inc_upload());
  need_resume_inc_upload = false;
  ASSERT_EQ(OB_SUCCESS, cleanup_tx_data_replace_test());
  need_cleanup_test = false;
}


TEST_F(ObSharedStorageTest, test_tx_data_gc)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t tablet_version1;
  int64_t tablet_version2;
  int64_t tablet_version3;
  EXE_SQL("alter system set _tx_result_retention = 0");
  EXE_SQL("alter system set ob_compaction_schedule_interval = '3s'");
  EXE_SQL("create table test_table (a int)");
  LOG_INFO("create_table finish");

  set_ls_and_tablet_id_for_run_ctx();

  EXE_SQL("insert into test_table values (1)");
  LOG_INFO("insert data finish1");
  ob_usleep(1 * 1000 * 1000);
  int64_t row_scn;
  get_row_scn("1", row_scn);

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();

  EXE_SQL("insert into test_table values (2)");
  LOG_INFO("insert data finish2");
  ob_usleep(1 * 1000 * 1000);
  int64_t row_scn2;
  get_row_scn("2", row_scn2);

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();

  EXE_SQL("insert into test_table values (3)");
  LOG_INFO("insert data finish3");

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();

  ob_usleep(40 * 1000 * 1000);

  ASSERT_GE(tx_data_recycle_scn, row_scn);

  EXE_SQL("insert into test_table values (4)");
  LOG_INFO("insert data finish4");

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();

  EXE_SQL("insert into test_table values (5)");
  LOG_INFO("insert data finish4");

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();

  EXE_SQL("insert into test_table values (6)");
  LOG_INFO("insert data finish4");

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();

  ob_usleep(40 * 1000 * 1000);

  ASSERT_GE(tx_data_recycle_scn, row_scn2);

}

void ObSharedStorageTest::get_row_scn(const char *value,
                                      int64_t &row_scn)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t affected_rows = 0;

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select ORA_ROWSCN as row_scn from test_table where a = %s", value));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("row_scn", row_scn));
  }

  LOG_INFO("get row scn", K(row_scn));
}

void ObSharedStorageTest::wait_minor_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait minor begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=0;", 
          RunCtx.tenant_id_, RunCtx.tablet_id_.id()));
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

void ObSharedStorageTest::set_ls_and_tablet_id_for_run_ctx(const char *table_name)
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t uid = 0;
  int64_t id = 0;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select tablet_id from oceanbase.__all_virtual_table where table_name='%s';", table_name));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_uint("tablet_id", uid));
  }
  RunCtx.tablet_id_ = uid;

  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select ls_id from oceanbase.__all_tablet_to_ls where tablet_id=%ld;", uid));
  SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res2, sql.ptr()));
    sqlclient::ObMySQLResult *result = res2.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", id));
  }
  ObLSID ls_id(id);

  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::TXSTORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  RunCtx.ls_id_ = ls->get_ls_id();
  RunCtx.ls_epoch_ = ls->get_ls_epoch();
  RunCtx.tenant_epoch_ = MTL_EPOCH_ID();
  LOG_INFO("finish set run ctx", K(RunCtx.tenant_epoch_), K(RunCtx.ls_id_), K(RunCtx.ls_epoch_), K(RunCtx.tablet_id_));
}

TEST_F(ObSharedStorageTest, end)
{
  ObSqlString sql;
  int64_t affected_rows = 0;
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  EXE_SQL("alter system set minor_compact_trigger = 2");
  EXE_SQL("alter system set _minor_merge_write_amplification_threshold = 2000000");
  EXE_SQL("alter system set _minor_compaction_amplification_factor = 0");
  EXE_SQL("alter system set _shared_inc_sstable_upload_interval = '1s'");
  EXE_SQL("alter system set ob_compaction_schedule_interval = '3s'");
  EXE_SQL("drop table if exists test_tx_data_watermark");
  EXE_SQL("drop table if exists test_table");
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
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
  databuff_printf(buf, sizeof(buf), "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=2000&max_bandwidth=200000000B&scope=region", 
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
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
