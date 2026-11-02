/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Real-refresh observer integration test; requires a test object store.
// No synthetic TableStore or compaction-suite hooks.
#define USING_LOG_PREFIX STORAGETEST
#include <cstdlib>
#include <gtest/gtest.h>
#define protected public
#define private public
#include "lib/utility/ob_defer.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/async_index/ob_async_index_service.h"
#include "storage/async_index/ob_async_index_define.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_data.h"
#include "storage/blocksstable/ob_sstable.h"
#include "share/ob_async_index_refresh_util.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
char *shared_storage_info = nullptr;

namespace unittest
{
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;
using namespace compaction;

class ObAsyncIndexRefreshScnTest : public ObSimpleClusterTestBase
{
public:
  ObAsyncIndexRefreshScnTest()
      : ObSimpleClusterTestBase("test_async_index_refresh_scn", "50G", "50G", "50G"),
        tenant_id_(OB_INVALID_TENANT_ID)
  {}

  void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_EQ(OB_SUCCESS, create_tenant_with_retry("tt1", "5G", "10G", false, 4));
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
    ASSERT_NE(OB_INVALID_TENANT_ID, tenant_id_);
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
    ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    // Match shared-storage test tenant disk provisioning, without cache/GC tuning.
    ObTenantDiskSpaceManager *disk_mgr = MTL(ObTenantDiskSpaceManager *);
    ASSERT_NE(nullptr, disk_mgr);
    OB_SERVER_DISK_SPACE_MGR.hidden_sys_data_disk_config_size_ = 1024;
    int64_t configured_size = 0;
    int64_t actual_size = 0;
    bool resized = false;
    ASSERT_EQ(OB_SUCCESS, disk_mgr->get_tenant_unit_data_disk_size(
        tenant_id_, configured_size, actual_size));
    ASSERT_EQ(OB_SUCCESS, disk_mgr->resize_total_disk_size_(configured_size, resized));
    ASSERT_EQ(OB_SUCCESS, exe_sql("alter system set _enable_async_index = true"));
    ASSERT_EQ(OB_SUCCESS, exe_sql("alter system set _enable_async_index_refresh = true"));
    ASSERT_EQ(OB_SUCCESS, exe_sql("set global ob_query_timeout = 120000000"));
    ASSERT_EQ(OB_SUCCESS, exe_sql("set global ob_trx_timeout = 120000000"));
  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

  int exe_sql(const char *sql)
  {
    int64_t affected_rows = 0;
    return get_curr_simple_server().get_sql_proxy2().write(sql, affected_rows);
  }

  void disable_and_drain_async_index_refresh();
  int get_data_table_id(sqlclient::ObISQLConnection &conn, const char *table_name, int64_t &data_table_id);
  int get_tablet_and_ls_id_by_table_id(
      ObMySQLProxy &proxy, const int64_t table_id, ObTabletID &tablet_id, ObLSID &ls_id);
  int get_tablet_handle(const ObLSID &ls_id, const ObTabletID &tablet_id, ObTabletHandle &tablet_handle);

  uint64_t tenant_id_;
};

void ObAsyncIndexRefreshScnTest::disable_and_drain_async_index_refresh()
{
  // Drain DDL bootstrap before issuing a refresh with an explicitly known GTS.
  ASSERT_EQ(OB_SUCCESS, exe_sql("alter system set _enable_async_index_refresh = false"));
  ObAsyncIndexService *service = MTL(ObAsyncIndexService *);
  ASSERT_NE(nullptr, service);
  ObAsyncIndexRefreshWorker &worker = service->get_refresher().get_refresh_worker();
  const int64_t deadline_us = ObTimeUtility::current_time() + 30L * 1000 * 1000;
  while (worker.get_in_flight_count() > 0 && ObTimeUtility::current_time() < deadline_us) {
    ob_usleep(100 * 1000);
  }
  ASSERT_EQ(0, worker.get_in_flight_count());
}

int ObAsyncIndexRefreshScnTest::get_data_table_id(
    sqlclient::ObISQLConnection &conn, const char *table_name, int64_t &data_table_id)
{
  int ret = OB_SUCCESS;
  data_table_id = 0;
  ObSqlString sql;
  sql.assign_fmt("select table_id as val from oceanbase.__all_virtual_table "
                 "where table_name = '%s' and data_table_id = 0 limit 1",
                 table_name);
  if (OB_FAIL(SimpleServerHelper::select_int64(&conn, sql.ptr(), data_table_id))) {
    LOG_WARN("failed to get data table id", KR(ret), K(sql));
  }
  return ret;
}

int ObAsyncIndexRefreshScnTest::get_tablet_and_ls_id_by_table_id(
    common::ObMySQLProxy &sql_proxy,
    const int64_t table_id,
    ObTabletID &tablet_id,
    ObLSID &ls_id)
{
  int ret = OB_ENTRY_NOT_EXIST;
  int64_t tmp_tablet_id = 0;
  int64_t tmp_ls_id = 0;
  ObSqlString sql;
  for (int64_t i = 0; i < 60; ++i) {
    tmp_tablet_id = 0;
    tmp_ls_id = 0;
    sql.reuse();
    sql.assign_fmt("select tablet_id as val from oceanbase.__all_virtual_table "
                   "where table_id = %ld limit 1", table_id);
    if (OB_FAIL(SimpleServerHelper::select_int64(sql_proxy, sql.ptr(), tmp_tablet_id))) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      sql.reuse();
      sql.assign_fmt("select ls_id as val from oceanbase.__all_tablet_to_ls "
                     "where tablet_id = %ld limit 1", tmp_tablet_id);
      if (OB_FAIL(SimpleServerHelper::select_int64(sql_proxy, sql.ptr(), tmp_ls_id))) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        tablet_id = ObTabletID(tmp_tablet_id);
        ls_id = ObLSID(tmp_ls_id);
        ret = OB_SUCCESS;
        break;
      }
    }
    sleep(1);
  }
  return ret;
}

int ObAsyncIndexRefreshScnTest::get_tablet_handle(
    const ObLSID &ls_id, const ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = MTL(ObLSService *);
  ObLSHandle ls_handle;
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", KR(ret), K(tablet_id));
  }
  return ret;
}

// Exercise the real refresh/DDL index rebuild path, not hand-built SSTables.
// The request supplies the logical snapshot; transaction-keyed MDS supplies
// the independent IncStart/IncCommit SCNs. Do not derive expectations from
// the output TableKey (or compare two replicas that could both be wrong).
TEST_F(ObAsyncIndexRefreshScnTest, refresh_ddl_physical_scn_and_macro_snapshot)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  const char *table_name = "t_refresh_ddl_scn_contract";
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "create table t_refresh_ddl_scn_contract (c1 int, c2 varchar(128), "
      "async fulltext index idx_a(c2)) organization heap"));
  DEFER(EXPECT_EQ(OB_SUCCESS, exe_sql("drop table t_refresh_ddl_scn_contract")));
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "alter table t_refresh_ddl_scn_contract set async_index_params = 'ASYNC_REFRESH_INTERVAL=-1'"));
  // Drain bootstrap work without removing its output or installing mock tables.
  ASSERT_NO_FATAL_FAILURE(disable_and_drain_async_index_refresh());
  ASSERT_EQ(OB_SUCCESS, exe_sql("alter system set _enable_async_index_refresh = true"));

#ifdef ERRSIM
  const int64_t saved_macro_ids_threshold = GCONF.errsim_storage_meta_macro_ids_threshold;
  DEFER(GCONF.errsim_storage_meta_macro_ids_threshold = saved_macro_ids_threshold);
  GCONF.errsim_storage_meta_macro_ids_threshold = 1;
#endif

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);
  DEFER(EXPECT_EQ(OB_SUCCESS, sql_proxy.close(conn, OB_SUCCESS)));
  int64_t data_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_data_table_id(*conn, table_name, data_table_id));
  ObSEArray<uint64_t, 4> index_ids;
  ASSERT_EQ(OB_SUCCESS, share::ObAsyncIndexRefreshUtil::collect_async_index_table_ids(
      tenant_id_, data_table_id, index_ids));
  ASSERT_EQ(2, index_ids.count()); // inverted index AND ordinary row-store delete index
  constexpr int64_t DOCUMENT_COUNT = 8000;
  const auto insert_documents = [&](const int64_t generation) {
    // Distinct terms and thousands of documents exercise an external index
    // tree; a one-row example can keep every index block inline and never
    // allocate a macro block through the descriptor under review.
    for (int64_t first = 0; first < DOCUMENT_COUNT; first += 100) {
      ObSqlString insert;
      ASSERT_EQ(OB_SUCCESS, insert.assign("insert into t_refresh_ddl_scn_contract values "));
      for (int64_t row = first; row < first + 100; ++row) {
        const int64_t id = generation * DOCUMENT_COUNT + row;
        ASSERT_EQ(OB_SUCCESS, insert.append_fmt("%s(%ld, '", row == first ? "" : ",", id));
        for (int64_t term = 0; term < 8; ++term) {
          ASSERT_EQ(OB_SUCCESS, insert.append_fmt("word%ldx%ld ", id, term));
        }
        ASSERT_EQ(OB_SUCCESS, insert.append("')"));
      }
      ASSERT_EQ(OB_SUCCESS, exe_sql(insert.ptr()));
    }
  };
  // Seed deletion candidates before the FIRST refresh as well. Async refresh
  // emits insert-then-delete rows even when they were not alive at the base
  // snapshot (final_delete && !base_live in ObMviewIncrMerge).
  ASSERT_NO_FATAL_FAILURE(insert_documents(0));
  int64_t previous_target = 0;
  for (int64_t round = 0; round < 2; ++round) {
    SCOPED_TRACE(round);
    // Both refresh targets must have real input in every round: deleted old
    // documents for the delete index and new documents for the inverted index.
    ASSERT_EQ(OB_SUCCESS, exe_sql("delete from t_refresh_ddl_scn_contract"));
    ASSERT_NO_FATAL_FAILURE(insert_documents(round + 1));

    SCN target;
    transaction::MonotonicTs received(0);
    ASSERT_EQ(OB_SUCCESS, OB_TS_MGR.get_gts_sync(tenant_id_,
        transaction::MonotonicTs::current_time(), 10 * 1000000L, target, received));
    const int64_t target_snapshot = target.get_val_for_tx();
    ASSERT_GT(target_snapshot, previous_target);
    // Same production entry as DBMS_ASYNC_INDEX.REFRESH, with a known GTS.
    ASSERT_EQ(OB_SUCCESS, share::ObAsyncIndexRefreshUtil::trigger_refresh_for_table(
        tenant_id_, data_table_id, target_snapshot, index_ids,
        static_cast<int8_t>(ASYNC_REFRESH_TYPE_MANUAL)));

    int64_t inverted_count = 0;
    int64_t delete_count = 0;
    for (int64_t i = 0; i < index_ids.count(); ++i) {
      SCOPED_TRACE(index_ids.at(i));
      ObTabletID tablet_id;
      ObLSID ls_id;
      ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
          sql_proxy, index_ids.at(i), tablet_id, ls_id));
      ObTabletHandle tablet_handle;
      ObTabletMemberWrapper<ObTabletTableStore> store;
      ObSSTable *output = nullptr;
      const int64_t deadline = ObTimeUtility::current_time() + 120 * 1000000L;
      while (nullptr == output && ObTimeUtility::current_time() < deadline) {
        ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_id, tablet_id, tablet_handle));
        int64_t published = 0;
        ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_async_index_max_published_version(published));
        if (published >= target_snapshot) {
          ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->fetch_table_store(store));
          ASSERT_NE(nullptr, store.get_member());
          const ObSSTableArray &tables = store.get_member()->get_inc_major_sstables();
          for (int64_t j = 0; j < tables.count(); ++j) {
            ObSSTable *candidate = static_cast<ObSSTable *>(tables.at(j));
            if (target_snapshot == candidate->get_async_index_snapshot_version()) {
              ASSERT_EQ(nullptr, output) << "duplicate output for one refresh";
              output = candidate;
            }
          }
        }
        if (nullptr == output) {
          ob_usleep(100 * 1000);
        }
      }
      ASSERT_NE(nullptr, output) << "refresh did not publish the requested snapshot";
      ObSqlString task_sql;
      ASSERT_EQ(OB_SUCCESS, task_sql.assign_fmt(
          "select task_id as val from oceanbase.__all_virtual_async_index_task_info "
          "where tenant_id = %lu and tablet_id = %ld and trigger_type = %d "
          "and target_refresh_snapshot = %lu and ret_code = 0 and end_time is not null "
          "order by end_time desc limit 1",
          tenant_id_, tablet_id.id(), static_cast<int>(ASYNC_REFRESH_TYPE_MANUAL),
          target.get_val_for_inner_table_field()));
      int64_t refresh_task_id = 0;
      ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(
          conn, task_sql.ptr(), refresh_task_id));
      ASSERT_GT(refresh_task_id, 0);
      ASSERT_NE(target_snapshot, refresh_task_id);
      const bool is_delete = tablet_handle.get_obj()->is_async_delete_index();
      if (is_delete) {
        ++delete_count;
        ASSERT_TRUE(output->is_inc_major_sstable());
      } else {
        ++inverted_count;
        ASSERT_TRUE(output->is_inc_inv_sstable());
      }
      transaction::ObTransID trans_id;
      transaction::ObTxSEQ seq_no;
      ASSERT_EQ(OB_SUCCESS, ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(
          output, trans_id, seq_no));
      ObArenaAllocator mds_allocator(ObMemAttr(tenant_id_, "RefreshScnTest"));
      ObTabletDDLCompleteMdsUserData start_info;
      ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_inc_major_direct_load_info(
          SCN::max_scn(), mds_allocator,
          ObTabletDDLCompleteMdsUserDataKey(trans_id), start_info));
      ASSERT_TRUE(start_info.start_scn_.is_valid_and_not_min());
      ASSERT_TRUE(start_info.inc_major_commit_scn_.is_valid_and_not_min());
      EXPECT_EQ(target_snapshot, start_info.snapshot_version_);
      EXPECT_EQ(start_info.start_scn_, output->get_start_scn());
      EXPECT_GT(output->get_start_scn(), target);
      EXPECT_LE(output->get_end_scn(), start_info.inc_major_commit_scn_);
      EXPECT_NE(target, output->get_end_scn());
      EXPECT_EQ(target_snapshot, output->get_async_index_snapshot_version());
      if (round > 0) {
        EXPECT_EQ(previous_target, output->get_async_index_base_version());
      }
      ObSSTableMetaHandle meta;
      ASSERT_EQ(OB_SUCCESS, output->get_meta(meta));
      EXPECT_EQ(target_snapshot, meta.get_sstable_meta().get_basic_meta().create_snapshot_version_);
      EXPECT_GT(meta.get_sstable_meta().get_row_count(), 0);
      EXPECT_GT(meta.get_sstable_meta().get_data_macro_block_count(), 0);
      EXPECT_GT(output->get_end_scn(), output->get_start_scn());
      const ObSSTableMacroInfo &macros = meta.get_sstable_meta().get_macro_info();
      ObMacroIdIterator first_data_ids;
      ASSERT_EQ(OB_SUCCESS, macros.get_data_block_iter(first_data_ids));
      MacroBlockId first_data_id;
      ASSERT_EQ(OB_SUCCESS, first_data_ids.get_next_macro_id(first_data_id));
      ASSERT_EQ(ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO,
          first_data_id.storage_object_type());
      const int64_t observed_fourth_id = first_data_id.fourth_id();
      ASSERT_GT(observed_fourth_id, 0);
      EXPECT_NE(refresh_task_id, observed_fourth_id);
      EXPECT_NE(target_snapshot, observed_fourth_id);
      // Check persisted IDs from both data writing and final index rebuilding.
      for (int kind = 0; kind < 2; ++kind) {
        ObMacroIdIterator ids;
        ASSERT_EQ(OB_SUCCESS, 0 == kind ? macros.get_data_block_iter(ids)
                                     : macros.get_other_block_iter(ids));
        MacroBlockId id;
        int iter_ret = OB_SUCCESS;
        int64_t count = 0;
        while (OB_SUCCESS == (iter_ret = ids.get_next_macro_id(id))) {
          ++count;
          EXPECT_EQ(observed_fourth_id, id.fourth_id());
          EXPECT_EQ(0 == kind ? ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO
                              : ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO,
              id.storage_object_type());
        }
        ASSERT_EQ(OB_ITER_END, iter_ret);
        if (0 == kind || !is_delete) {
          EXPECT_GT(count, 0)
              << "coverage prerequisite: data blocks on both targets and external inverted index blocks";
        }
      }
#ifdef ERRSIM
      ASSERT_FALSE(output->is_small_sstable());
      ObMacroIdIterator linked_ids;
      ASSERT_EQ(OB_SUCCESS, macros.get_linked_block_iter(linked_ids));
      MacroBlockId linked_id;
      int linked_iter_ret = OB_SUCCESS;
      int64_t linked_count = 0;
      while (OB_SUCCESS == (linked_iter_ret = linked_ids.get_next_macro_id(linked_id))) {
        ++linked_count;
        EXPECT_EQ(observed_fourth_id, linked_id.fourth_id());
      }
      ASSERT_EQ(OB_ITER_END, linked_iter_ret);
      EXPECT_GT(linked_count, 0)
          << "coverage prerequisite: write_partial_sstable persisted the macro-ID list";
#endif
    }
    ASSERT_EQ(1, inverted_count);
    ASSERT_EQ(1, delete_count);
    previous_target = target_snapshot;
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  using namespace oceanbase::common;
  const char *s3_bucket = std::getenv("OB_MITTEST_S3_BUCKET");
  if (OB_ISNULL(s3_bucket) || '\0' == s3_bucket[0]) {
    s3_bucket = oceanbase::unittest::S3_BUCKET;
  }
  const char *s3_endpoint_override = std::getenv("OB_MITTEST_S3_ENDPOINT");
  const bool use_test_endpoint = OB_NOT_NULL(s3_endpoint_override)
      && '\0' != s3_endpoint_override[0];
  const char *s3_endpoint = use_test_endpoint
      ? s3_endpoint_override : oceanbase::unittest::S3_ENDPOINT;
  // Endpoint override is only for local MinIO and must not accept real credentials.
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
      s3_bucket, cur_time_ns, s3_endpoint, s3_ak, s3_sk, s3_region);
  if (OB_SUCCESS != format_ret) {
    return format_ret;
  }
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
