/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Real-refresh observer integration test; requires a test object store.
// No synthetic TableStore or compaction-suite hooks.
#define USING_LOG_PREFIX STORAGETEST
#include <cstdlib>
#include <atomic>
#include <thread>
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
#include "storage/tx/ob_multi_data_source.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/async_index/ob_async_index_service.h"
#include "storage/async_index/ob_async_index_define.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_async_index_refresh_info_mds_helper.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_data.h"
#include "storage/blocksstable/ob_sstable.h"
#include "share/ob_async_index_refresh_util.h"
#include "share/ob_ddl_sim_point.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "rootserver/ddl_task/ob_table_redefinition_task.h"

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
    // 同一测试套件共用 server；后续用例复用租户和连接池，避免重复创建及初始化。
    if (!get_curr_simple_server().get_sql_proxy2().is_inited()) {
      ASSERT_EQ(OB_SUCCESS, create_tenant_with_retry("tt1", "5G", "10G", false, 4));
      ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
    }
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
    ASSERT_NE(OB_INVALID_TENANT_ID, tenant_id_);
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

#ifdef ERRSIM
  void check_first_online_index_snapshot(const bool use_alter);
  void check_offline_refresh_recovery(const int phase);
  void check_offline_refresh_route(const uint64_t version, const bool with_async_index);
  void read_offline_versions(const uint64_t table_id, ObIArray<int64_t> &physical,
                            ObIArray<int64_t> &logical, ObIArray<int64_t> &commit_versions);
#endif

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
// Compare the targeted builder against the full-table scheduler metadata, then
// exercise cache reuse, partition validation and invalidation with real schemas.
TEST_F(ObAsyncIndexRefreshScnTest, targeted_refresh_metadata_cache)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "create table t_refresh_metadata (id int primary key, body varchar(128), doc json, "
      "async fulltext index ft(body), async search index si(doc)) organization heap "
      "partition by hash(id) subpartition by hash(id) subpartitions 2 partitions 2"));
  DEFER({
    EXPECT_EQ(OB_SUCCESS, exe_sql("drop table if exists t_refresh_metadata"));
    EXPECT_EQ(OB_SUCCESS, exe_sql("drop table if exists t_refresh_metadata_renamed"));
  });
  auto &proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, proxy.acquire(conn));
  DEFER(EXPECT_EQ(OB_SUCCESS, proxy.close(conn, OB_SUCCESS)));
  int64_t table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_data_table_id(*conn, "t_refresh_metadata", table_id));
  ObArenaAllocator allocator;
  ObAsyncIndexDepProvider provider;
  ObAsyncIndexDataTableInfo full;
  ASSERT_EQ(OB_SUCCESS, provider.get_single_table_refresh_info(tenant_id_, table_id, allocator, full));
  ASSERT_EQ(4, full.data_tablet_infos_.count());
  ASSERT_EQ(3, full.index_infos_.count()); // FTS, Search Data, shared delete.
  const auto compare_columns = [](const ObIArray<ObString> &expected, const ObIArray<ObString> &actual) {
    ASSERT_EQ(expected.count(), actual.count());
    for (int64_t i = 0; i < expected.count(); ++i) {
      EXPECT_EQ(expected.at(i), actual.at(i));
    }
  };
  ObAsyncIndexRefreshMetadataCache cache;
  for (int64_t p = 0; p < full.data_tablet_infos_.count(); ++p) {
    const auto &data = full.data_tablet_infos_.at(p);
    for (int64_t i = 0; i < full.index_infos_.count(); ++i) {
      const auto &index = full.index_infos_.at(i);
      const ObAsyncIndexDataTableInfo *target = nullptr;
      ASSERT_EQ(OB_SUCCESS, cache.get(tenant_id_, table_id, data.data_tablet_id_,
          index.index_table_id_, index.tablet_ids_.at(p), target));
      ASSERT_NE(nullptr, target);
      ASSERT_EQ(1, target->data_tablet_infos_.count());
      ASSERT_EQ(1, target->index_infos_.count());
      EXPECT_EQ(full.database_name_, target->database_name_);
      EXPECT_EQ(full.data_table_name_, target->data_table_name_);
      EXPECT_EQ(full.schema_version_, target->schema_version_);
      EXPECT_EQ(data.partition_name_, target->data_tablet_infos_.at(0).partition_name_);
      const auto &actual = target->index_infos_.at(0);
      EXPECT_EQ(index.index_table_name_, actual.index_table_name_);
      EXPECT_EQ(index.def_index_table_id_, actual.def_index_table_id_);
      EXPECT_EQ(index.index_type_, actual.index_type_);
      EXPECT_EQ(index.enable_newsort_, actual.enable_newsort_);
      EXPECT_EQ(index.is_delete_index_, actual.is_delete_index_);
      EXPECT_EQ(index.is_refresh_on_error_skip_, actual.is_refresh_on_error_skip_);
      EXPECT_EQ(index.tablet_ids_.at(p), actual.tablet_ids_.at(0));
      EXPECT_EQ(index.partition_names_.at(p), actual.partition_names_.at(0));
      ASSERT_NO_FATAL_FAILURE(compare_columns(index.select_columns_, actual.select_columns_));
      ASSERT_NO_FATAL_FAILURE(compare_columns(index.source_columns_, actual.source_columns_));
      ASSERT_NO_FATAL_FAILURE(compare_columns(index.partition_key_columns_, actual.partition_key_columns_));
      ASSERT_NO_FATAL_FAILURE(compare_columns(index.rowkey_columns_, actual.rowkey_columns_));
      ASSERT_NO_FATAL_FAILURE(compare_columns(index.order_columns_, actual.order_columns_));
      const ObAsyncIndexDataTableInfo *reused = nullptr;
      const int64_t version = cache.schema_version_;
      ASSERT_EQ(OB_SUCCESS, cache.get(tenant_id_, table_id, data.data_tablet_id_,
          index.index_table_id_, index.tablet_ids_.at(p), reused));
      if (version == cache.schema_version_) {
        EXPECT_EQ(target, reused);
      }
    }
  }
  const auto &index = full.index_infos_.at(0);
  const ObTabletID data_tablet = full.data_tablet_infos_.at(0).data_tablet_id_;
  const ObAsyncIndexDataTableInfo *target = nullptr;
  EXPECT_EQ(OB_SCHEMA_EAGAIN, cache.get(tenant_id_, table_id, data_tablet,
      index.index_table_id_, index.tablet_ids_.at(1), target));
  EXPECT_EQ(nullptr, target);
  ASSERT_EQ(OB_SUCCESS, cache.get(tenant_id_, table_id, data_tablet,
      index.index_table_id_, index.tablet_ids_.at(0), target));
  const int64_t old_version = cache.schema_version_;
  ASSERT_EQ(OB_SUCCESS, exe_sql("rename table t_refresh_metadata to t_refresh_metadata_renamed"));
  ASSERT_EQ(OB_SUCCESS, cache.get(tenant_id_, table_id, data_tablet,
      index.index_table_id_, index.tablet_ids_.at(0), target));
  EXPECT_NE(old_version, cache.schema_version_);
  EXPECT_EQ(ObString::make_string("t_refresh_metadata_renamed"), target->data_table_name_);
}

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

// 真实 offline DDL 刷新到逻辑 R 并提交 D；空刷允许物理水位落后于 R。
TEST_F(ObAsyncIndexRefreshScnTest, real_offline_publication_refresh_start_under_pending_mds)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  const char *drop_sql = "drop table if exists t_real_offline_refresh_start";
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(drop_sql, affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(
      "create table t_real_offline_refresh_start ("
      "id int primary key, body varchar(256), async fulltext index idx_ft(body)) "
      "organization heap default charset=utf8mb4 collate=utf8mb4_general_ci "
      "async_index_params='ASYNC_REFRESH_INTERVAL=-1'", affected_rows));
  DEFER(EXPECT_EQ(OB_SUCCESS, sql_proxy.write(drop_sql, affected_rows)));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(
      "insert into t_real_offline_refresh_start values (1, 'offline seed')", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(
      "alter table t_real_offline_refresh_start "
      "convert to character set utf8mb4 collate utf8mb4_bin", affected_rows));

  // 交换后重新定位真实倒排索引 tablet；不用旧表 ID，也不修改 tablet cache。
  ObSqlString sql;
  int64_t tablet_id_value = 0;
  int64_t ls_id_value = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select i.tablet_id as val from oceanbase.__all_virtual_table i "
      "join oceanbase.__all_virtual_table d "
      "on i.tenant_id = d.tenant_id and i.data_table_id = d.table_id "
      "where d.tenant_id = %lu and d.table_name = 't_real_offline_refresh_start' "
      "and i.index_type = 47", tenant_id_));
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(sql_proxy, sql.ptr(), tablet_id_value));
  ASSERT_GT(tablet_id_value, 0);
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select ls_id as val from oceanbase.__all_tablet_to_ls where tablet_id = %ld",
      tablet_id_value));
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(sql_proxy, sql.ptr(), ls_id_value));
  const ObTabletID tablet_id(tablet_id_value);
  const ObLSID ls_id(ls_id_value);

  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id_) {
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    ObTablet *tablet = tablet_handle.get_obj();
    ASSERT_NE(nullptr, tablet);
    ASSERT_TRUE(tablet->is_async_index_refresh_target());

    ObTabletAsyncIndexRefreshInfo publication;
    mds::MdsWriter writer;
    mds::TwoPhaseCommitState state;
    SCN commit_version;
    ASSERT_EQ(OB_SUCCESS, tablet->mds_get_row_(
        mds::DummyKey(), SCN::max_scn(), 1_s, false /* read_uncommitted */,
        publication, nullptr, writer, state, commit_version));
    ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, state);
    ASSERT_TRUE(publication.is_valid());
    ASSERT_TRUE(commit_version.is_valid());
    int64_t physical_version = 0;
    ASSERT_EQ(OB_SUCCESS, tablet->get_async_index_max_published_version(physical_version));
    const int64_t publish_version = publication.get_published_version();
    ASSERT_GT(physical_version, 0);
    ASSERT_LT(physical_version, publish_version);
    ASSERT_EQ(publish_version, commit_version.get_val_for_tx());

    const auto check_versions = [&]() {
      ObTabletAsyncIndexRefreshInfo committed_info;
      mds::MdsWriter committed_writer;
      mds::TwoPhaseCommitState committed_state;
      SCN committed_version;
      ASSERT_EQ(OB_SUCCESS, tablet->mds_get_row_(
          mds::DummyKey(), SCN::max_scn(), 1_s, false /* read_uncommitted */,
          committed_info, nullptr, committed_writer, committed_state, committed_version));
      ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, committed_state);
      ASSERT_EQ(publish_version, committed_info.get_published_version());
      ASSERT_EQ(publish_version, committed_version.get_val_for_tx());
      int64_t current_physical = 0;
      int64_t refresh_start = 0;
      int64_t query_version = 0;
      ASSERT_EQ(OB_SUCCESS, tablet->get_async_index_max_published_version(current_physical));
      ASSERT_EQ(physical_version, current_physical);
      ASSERT_EQ(OB_SUCCESS, tablet->get_async_index_refresh_start_version(refresh_start, false /* use_cache */));
      ASSERT_EQ(publish_version, refresh_start);
      ASSERT_EQ(OB_SUCCESS, tablet->get_async_index_query_published_version(
          SCN::max_scn(), query_version));
      ASSERT_EQ(publish_version, query_version);
    };
    ASSERT_NO_FATAL_FAILURE(check_versions());
    // The swap precondition travels in the registration argument, not in the MDS value.
    {
      ObTabletAsyncIndexRefreshInfoArg arg;
      arg.ls_id_ = ls_id;
      arg.tablet_id_ = tablet_id;
      arg.expected_published_version_ = publish_version;
      arg.info_.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_VERSION);
      ASSERT_TRUE(arg.is_valid());
      char buf[512];
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, arg.serialize(buf, sizeof(buf), pos));
      ObTabletAsyncIndexRefreshInfoArg decoded;
      int64_t read_pos = 0;
      ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, read_pos));
      ASSERT_EQ(pos, read_pos);
      ASSERT_TRUE(decoded.is_valid());
      ASSERT_EQ(publish_version, decoded.expected_published_version_);
      ASSERT_TRUE(decoded.info_.need_fill_commit_version());
    }
    for (int64_t offset = 0; offset < 2; ++offset) {
      SCOPED_TRACE(offset);
      ObTabletAsyncIndexRefreshInfo pending_info;
      pending_info.set_published_version(publish_version + offset);
      ObMySQLTransaction pending_trans;
      ASSERT_EQ(OB_SUCCESS, pending_trans.start(GCTX.sql_proxy_, tenant_id_));
      DEFER(if (pending_trans.is_started()) {
        EXPECT_EQ(OB_SUCCESS, pending_trans.end(false /* commit */));
      });
      ObTabletAsyncIndexRefreshInfoArg arg;
      arg.ls_id_ = ls_id;
      arg.tablet_id_ = tablet_id;
      ASSERT_EQ(OB_SUCCESS, arg.info_.assign(pending_info));
      char buf[512];
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, arg.serialize(buf, sizeof(buf), pos));
      auto *conn = static_cast<observer::ObInnerSQLConnection *>(pending_trans.get_connection());
      ASSERT_NE(nullptr, conn);
      transaction::ObRegisterMdsFlag flag;
      ASSERT_EQ(OB_SUCCESS, conn->register_multi_data_source(
          tenant_id_, ls_id, transaction::ObTxDataSourceType::ASYNC_INDEX_REFRESH_INFO,
          buf, pos, flag));

      // 确认覆盖层确实存在且尚未提交，避免未制造并发窗口却假通过。
      ObTabletAsyncIndexRefreshInfo latest_info;
      ASSERT_EQ(OB_SUCCESS, tablet->get_latest_async_index_refresh_info(
          latest_info, writer, state, commit_version));
      ASSERT_NE(mds::TwoPhaseCommitState::ON_COMMIT, state);
      ASSERT_EQ(publish_version + offset, latest_info.get_published_version());
      ASSERT_NO_FATAL_FAILURE(check_versions());
      ASSERT_EQ(OB_SUCCESS, pending_trans.end(false /* commit */));
      ASSERT_NO_FATAL_FAILURE(check_versions());
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}


// Exercise the LS offline/online replay path with a real committed table swap.
// This is LS recovery coverage, not an observer crash or RS failover test.
TEST_F(ObAsyncIndexRefreshScnTest, offline_publication_survives_ls_reboot_and_refresh)
{
  auto &proxy = get_curr_simple_server().get_sql_proxy2();
  const char *drop_sql = "drop table if exists t_offline_reboot";
  ASSERT_EQ(OB_SUCCESS, exe_sql(drop_sql));
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "create table t_offline_reboot (id int primary key, body varchar(256), "
      "async fulltext index ft(body)) organization heap "
      "default charset=utf8mb4 collate=utf8mb4_general_ci "
      "async_index_params='ASYNC_REFRESH_INTERVAL=-1'"));
  DEFER(EXPECT_EQ(OB_SUCCESS, exe_sql(drop_sql)));
  ASSERT_EQ(OB_SUCCESS, exe_sql("insert into t_offline_reboot values (1, 'reboot seed')"));
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "alter table t_offline_reboot convert to character set utf8mb4 collate utf8mb4_bin"));

  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  DEFER(EXPECT_EQ(OB_SUCCESS, exe_sql("alter system set _enable_async_index_refresh = true")));
  ASSERT_NO_FATAL_FAILURE(disable_and_drain_async_index_refresh());
  int64_t data_table_id = 0;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select table_id as val from oceanbase.__all_virtual_table "
      "where tenant_id=%lu and table_name='t_offline_reboot' and data_table_id=0", tenant_id_));
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(proxy, sql.ptr(), data_table_id));
  ASSERT_GT(data_table_id, 0);
  ObSEArray<uint64_t, 4> index_ids;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexRefreshUtil::collect_async_index_table_ids(
      tenant_id_, data_table_id, index_ids, true, true));
  ASSERT_EQ(2, index_ids.count()); // FTS and shared delete index.
  ObTabletID tablets[3];
  ObLSID ls_ids[3];
  ObTabletAsyncIndexRefreshInfo before[3];
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
        proxy, 0 == i ? data_table_id : index_ids.at(i - 1), tablets[i], ls_ids[i]));
    ASSERT_EQ(ls_ids[0], ls_ids[i]);
    ObTabletHandle handle;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_ids[i], tablets[i], handle));
    mds::MdsWriter writer;
    mds::TwoPhaseCommitState state;
    SCN commit_version;
    ASSERT_EQ(OB_SUCCESS, handle.get_obj()->mds_get_row_(
        mds::DummyKey(), SCN::max_scn(), 1_s, false /* read_uncommitted */,
        before[i], nullptr, writer, state, commit_version));
    ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, state);
    ASSERT_TRUE(before[i].is_valid());
    if (i > 0) {
      int64_t physical = 0;
      ASSERT_EQ(OB_SUCCESS, handle.get_obj()->get_async_index_max_published_version(physical));
      ASSERT_LT(physical, before[i].get_published_version());
      ASSERT_EQ(before[1].get_published_version(), before[i].get_published_version());
      ASSERT_EQ(commit_version.get_val_for_tx(), before[i].get_published_version());
    } else {
      ASSERT_TRUE(before[i].has_async_index());
      ASSERT_FALSE(before[i].get_refresh_snapshots().empty());
    }
  } // Release every tablet/LS handle before reboot.

  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::freeze_tx_ctx(tenant_id_, ls_ids[0]));
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::ls_reboot(tenant_id_, ls_ids[0]));
  for (int64_t i = 0; i < 3; ++i) {
    SCOPED_TRACE(i);
    ObTabletHandle handle;
    ObTabletAsyncIndexRefreshInfo after;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_ids[i], tablets[i], handle));
    ASSERT_EQ(OB_SUCCESS, handle.get_obj()->get_latest_committed_async_index_refresh_info(after));
    ASSERT_TRUE(after.is_valid());
    ASSERT_EQ(before[i].has_async_index(), after.has_async_index());
    ASSERT_EQ(before[i].get_published_version(), after.get_published_version());
    ASSERT_EQ(before[i].get_refresh_snapshots().count(), after.get_refresh_snapshots().count());
    for (int64_t j = 0; j < before[i].get_refresh_snapshots().count(); ++j) {
      ASSERT_EQ(before[i].get_refresh_snapshots().at(j), after.get_refresh_snapshots().at(j));
    }
    if (i > 0) {
      int64_t start = 0;
      int64_t query = 0;
      ASSERT_EQ(OB_SUCCESS, handle.get_obj()->get_async_index_refresh_start_version(start, false));
      ASSERT_EQ(after.get_published_version(), start);
      ASSERT_EQ(OB_SUCCESS, handle.get_obj()->get_async_index_query_published_version(SCN::max_scn(), query));
      ASSERT_EQ(start, query);
    }
  }
  const char *query_sql = "select count(*) as val from hybrid_search(TABLE t_offline_reboot, "
      "'{\"query\":{\"match\":{\"body\":\"reboot\"}}}')";
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(proxy, query_sql, count));
  ASSERT_EQ(1, count); // Query before any post-reboot refresh.
  ASSERT_EQ(OB_SUCCESS, exe_sql("alter system set _enable_async_index_refresh = true"));
  ASSERT_EQ(OB_SUCCESS, exe_sql("delete from t_offline_reboot where id=1"));
  ASSERT_EQ(OB_SUCCESS, exe_sql("insert into t_offline_reboot values (2, 'reboot next')"));
  ASSERT_EQ(OB_SUCCESS, exe_sql("call dbms_async_index.refresh('t_offline_reboot')"));
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(proxy, query_sql, count));
  ASSERT_EQ(1, count);
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(proxy,
      "select id as val from hybrid_search(TABLE t_offline_reboot, "
      "'{\"query\":{\"match\":{\"body\":\"reboot\"}}}')", count));
  ASSERT_EQ(2, count); // Old deletion and new insertion both took effect.
  for (int64_t i = 1; i < 3; ++i) {
    ObTabletHandle handle;
    ObTabletAsyncIndexRefreshInfo after;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_ids[i], tablets[i], handle));
    ASSERT_EQ(OB_SUCCESS, handle.get_obj()->get_latest_committed_async_index_refresh_info(after));
    ASSERT_GT(after.get_published_version(), before[i].get_published_version());
  }
}


#ifdef ERRSIM
// 读取全部索引分区，不以第一个 tablet 的结果代表整表完成。
void ObAsyncIndexRefreshScnTest::read_offline_versions(
    const uint64_t table_id, ObIArray<int64_t> &physical,
    ObIArray<int64_t> &logical, ObIArray<int64_t> &commit_versions)
{
  ObSEArray<uint64_t, 4> index_ids;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexRefreshUtil::collect_async_index_table_ids(
      tenant_id_, table_id, index_ids, true, true));
  ASSERT_FALSE(index_ids.empty());
  physical.reset();
  logical.reset();
  commit_versions.reset();
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  for (int64_t i = 0; i < index_ids.count(); ++i) {
    ObSEArray<ObTabletID, 4> tablets;
    ObSEArray<ObLSID, 4> ls_ids;
    ASSERT_EQ(OB_SUCCESS, ObDDLUtil::get_tablets(tenant_id_, index_ids.at(i), tablets));
    ASSERT_EQ(2, tablets.count());
    ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get_ls(
        *GCTX.sql_proxy_, tenant_id_, tablets, ls_ids));
    ASSERT_EQ(tablets.count(), ls_ids.count());
    for (int64_t j = 0; j < tablets.count(); ++j) {
      ObTabletHandle handle;
      ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_ids.at(j), tablets.at(j), handle));
      int64_t pv = 0;
      ASSERT_EQ(OB_SUCCESS, handle.get_obj()->get_async_index_max_published_version(pv));
      ASSERT_EQ(OB_SUCCESS, physical.push_back(pv));
      ObTabletAsyncIndexRefreshInfo info;
      mds::MdsWriter writer;
      mds::TwoPhaseCommitState state;
      SCN commit_scn;
      const int mds_ret = handle.get_obj()->mds_get_row_(
          mds::DummyKey(), SCN::max_scn(), 1_s, false, info, nullptr, writer, state, commit_scn);
      // 尚未刷新过的隐藏索引可以没有 MDS 行；发布后调用方必须断言 D > R。
      ASSERT_TRUE(OB_SUCCESS == mds_ret || OB_EMPTY_RESULT == mds_ret) << mds_ret;
      if (OB_SUCCESS == mds_ret) {
        ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, state);
        ASSERT_TRUE(info.is_valid());
      }
      ASSERT_EQ(OB_SUCCESS, logical.push_back(
          info.has_published_version() ? info.get_published_version() : 0));
      ASSERT_EQ(OB_SUCCESS, commit_versions.push_back(
          OB_SUCCESS == mds_ret ? commit_scn.get_val_for_tx() : 0));
    }
  }
}

// phase 0: R 已落盘但未刷新；1: 一个分区暂停、其他分区完成；2: swap 已提交但任务尚未转 SUCCESS；3: 消息保存持续失败。
// 暂停原任务后从任务表重建对象调用真实状态实现，覆盖任务恢复而非冒充真实 RS 切主。
void ObAsyncIndexRefreshScnTest::check_offline_refresh_recovery(const int phase)
{
  using namespace rootserver;
  auto &proxy = get_curr_simple_server().get_sql_proxy2();
  const auto sys_sql = [&](const char *sql) {
    int64_t affected = 0;
    return exec_write_sql_sys(sql, affected);
  };
  uint64_t data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(tenant_id_, data_version));
  ASSERT_GE(data_version, DATA_VERSION_4_6_3_0);
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table if exists t_offline_stage_recovery"));
  if (0 == phase) {
    ASSERT_EQ(OB_SUCCESS, exe_sql("drop table if exists t_offline_stage_unrelated"));
  }
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "create table t_offline_stage_recovery (id int primary key, body varchar(256), "
      "async fulltext index ft(body)) organization heap partition by hash(id) partitions 2"));
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "alter table t_offline_stage_recovery async_index_params='ASYNC_REFRESH_INTERVAL=-1'"));
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "insert into t_offline_stage_recovery values (1, 'recovery first'), (2, 'recovery second')"));
  if (0 == phase) {
    ASSERT_EQ(OB_SUCCESS, exe_sql("create table t_offline_stage_unrelated (id int primary key)"));
  }
  int64_t original_id = 0;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select table_id as val from oceanbase.__all_virtual_table where tenant_id=%lu "
      "and table_name='t_offline_stage_recovery' and data_table_id=0", tenant_id_));
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(proxy, sql.ptr(), original_id));
  ASSERT_GT(original_id, 0);

  const char *points[] = {"BEFORE_OFFLINE_ASYNC_INDEX_REFRESH",
                        "BEFORE_OFFLINE_ASYNC_INDEX_PARTITION_REFRESH",
                        "TABLE_REDEFINITION_TAKE_EFFECT"};
  const int64_t previous_sync_timeout = static_cast<int64_t>(GCONF.debug_sync_timeout);
  DEFER({
    ObSqlString restore_sql;
    if (OB_SUCCESS == restore_sql.assign_fmt(
        "alter system set debug_sync_timeout='%ldus'", previous_sync_timeout)) {
      EXPECT_EQ(OB_SUCCESS, sys_sql(restore_sql.ptr()));
    }
  });
  ASSERT_EQ(OB_SUCCESS, sys_sql("alter system set debug_sync_timeout='120s'"));
  const int64_t config_deadline = ObTimeUtility::current_time() + 10_s;
  while (120_s != static_cast<int64_t>(GCONF.debug_sync_timeout)
         && ObTimeUtility::current_time() < config_deadline) {
    ob_usleep(10 * 1000);
  }
  ASSERT_EQ(120_s, static_cast<int64_t>(GCONF.debug_sync_timeout));
  ASSERT_EQ(OB_SUCCESS, sys_sql("set ob_global_debug_sync='reset'"));
  std::thread ddl_thread;
  std::thread unrelated_thread;
  std::atomic<int> ddl_ret(OB_EAGAIN);
  std::atomic<bool> ddl_done(false);
  std::atomic<int> unrelated_ret(OB_EAGAIN);
  std::atomic<bool> unrelated_done(false);
  // 断言失败也必须先解除同步点，再 join，避免遗留后台 SQL 或挂住测试进程。
  DEFER({
    EXPECT_EQ(OB_SUCCESS, sys_sql("set ob_global_debug_sync='now signal offline_stage_resume'"));
    EXPECT_EQ(OB_SUCCESS, sys_sql("set ob_global_debug_sync='reset'"));
    if (ddl_thread.joinable()) { ddl_thread.join(); }
    if (unrelated_thread.joinable()) { unrelated_thread.join(); }
    EXPECT_EQ(OB_SUCCESS, exe_sql("drop table if exists t_offline_stage_recovery"));
    if (0 == phase) {
      EXPECT_EQ(OB_SUCCESS, exe_sql("drop table if exists t_offline_stage_unrelated"));
    }
  });
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "set ob_global_debug_sync='%s signal offline_stage_paused "
      "wait_for offline_stage_resume execute 1'", points[3 == phase ? 0 : phase]));
  ASSERT_EQ(OB_SUCCESS, sys_sql(sql.ptr()));
  ddl_thread = std::thread([&]() {
    ddl_ret.store(exe_sql("alter table t_offline_stage_recovery "
                         "convert to character set utf8mb4 collate utf8mb4_bin"));
    ddl_done.store(true);
  });
  const int64_t wait_start = ObTimeUtility::current_time();
  // 直接在测试线程等待，避免 sys 租户 SQL 的短超时先于同步点超时生效。
  ObMalloc sync_allocator;
  ObDSSessionActions control_actions;
  ASSERT_EQ(OB_SUCCESS, control_actions.init(1024, sync_allocator));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync(
      "now wait_for offline_stage_paused timeout 120000000 execute 1",
      false /* is_global */, control_actions));
  // debug sync 超时也可能返回成功，不能仅检查接口返回码。
  ASSERT_LT(ObTimeUtility::current_time() - wait_start, 110_s);
  ASSERT_FALSE(ddl_done.load());

  ObArenaAllocator allocator;
  ObSEArray<ObDDLTaskRecord, 4> records;
  ASSERT_EQ(OB_SUCCESS, ObDDLTaskRecordOperator::get_ddl_task_record_by_table_id(
      tenant_id_, original_id, *GCTX.sql_proxy_, allocator, records));
  ASSERT_EQ(1, records.count());
  class MessageFailingTask final : public ObTableRedefinitionTask
  {
  public:
    int serialize_params_to_message(char *buf, const int64_t buf_len, int64_t &pos) const override
    {
      if (fail_next_message_) {
        fail_next_message_ = false;
        return OB_ALLOCATE_MEMORY_FAILED;
      }
      return ObTableRedefinitionTask::serialize_params_to_message(buf, buf_len, pos);
    }
    mutable bool fail_next_message_ = false;
  } recovered;
  ASSERT_EQ(OB_SUCCESS, recovered.init(records.at(0)));
  const int64_t refresh_snapshot = recovered.alter_table_arg_.async_index_offline_refresh_snapshot_;
  ASSERT_GT(refresh_snapshot, 0);
  ASSERT_EQ(2 == phase ? ObDDLTaskStatus::TAKE_EFFECT : ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH,
            recovered.get_task_status());
  if (3 == phase) {
    ASSERT_EQ(OB_SUCCESS, recovered.switch_status(ObDDLTaskStatus::MODIFY_AUTOINC, true, OB_SUCCESS));
    ASSERT_EQ(0, recovered.err_code_occurence_cnt_);
    // Only message preparation fails; state SQL and FAIL-message serialization remain available.
    for (int64_t i = 1; i <= 3; ++i) {
      recovered.fail_next_message_ = true;
      ASSERT_EQ(OB_SUCCESS, recovered.modify_autoinc(ObDDLTaskStatus::TAKE_EFFECT));
      ASSERT_FALSE(recovered.fail_next_message_);
      ASSERT_EQ(i, recovered.err_code_occurence_cnt_);
      ASSERT_EQ(i < 3 ? ObDDLTaskStatus::MODIFY_AUTOINC : ObDDLTaskStatus::FAIL,
                recovered.get_task_status());
      ASSERT_EQ(refresh_snapshot, recovered.alter_table_arg_.async_index_offline_refresh_snapshot_);
      ObDDLTaskRecord failed_record;
      ASSERT_EQ(OB_SUCCESS, ObDDLTaskRecordOperator::get_ddl_task_record(
          tenant_id_, recovered.task_id_, *GCTX.sql_proxy_, allocator, failed_record));
      ObTableRedefinitionTask reread;
      ASSERT_EQ(OB_SUCCESS, reread.init(failed_record));
      ASSERT_EQ(recovered.get_task_status(), reread.get_task_status());
      ASSERT_EQ(i < 3 ? OB_SUCCESS : OB_ALLOCATE_MEMORY_FAILED, reread.ret_code_);
    }
    ASSERT_EQ(OB_SUCCESS, sys_sql("set ob_global_debug_sync='now signal offline_stage_resume'"));
    ddl_thread.join();
    // The original executor may report cancellation after observing the persisted FAIL.
    ASSERT_NE(OB_SUCCESS, ddl_ret.load());
    return;
  }
  ObSEArray<int64_t, 8> physical_before;
  ObSEArray<int64_t, 8> logical_before;
  ObSEArray<int64_t, 8> commit_before;
  // Concurrent RPCs may finish after the paused partition signals the test.
  // Wait for another partition to finish while the paused RPC holds no ownership.
  const int64_t partial_refresh_deadline = ObTimeUtility::current_time() + 30_s;
  do {
    physical_before.reuse();
    logical_before.reuse();
    commit_before.reuse();
    ASSERT_NO_FATAL_FAILURE(read_offline_versions(
        recovered.target_object_id_, physical_before, logical_before, commit_before));
    bool has_finished = false;
    for (int64_t i = 0; i < logical_before.count(); ++i) {
      has_finished |= logical_before.at(i) >= refresh_snapshot;
    }
    if (1 != phase || has_finished || ObTimeUtility::current_time() >= partial_refresh_deadline) {
      break;
    }
    ob_usleep(10 * 1000);
  } while (true);
  ASSERT_GE(physical_before.count(), 2);
  int64_t finished = 0;
  for (int64_t i = 0; i < physical_before.count(); ++i) {
    ASSERT_LE(physical_before.at(i), refresh_snapshot);
    // Empty refreshes advance logical progress without installing an SSTable.
    finished += logical_before.at(i) >= refresh_snapshot ? 1 : 0;
    if (2 == phase) {
      ASSERT_GT(commit_before.at(i), refresh_snapshot);
      ASSERT_EQ(commit_before.at(i), logical_before.at(i));
    } else {
      ASSERT_LE(logical_before.at(i), refresh_snapshot);
    }
  }
  if (0 == phase) {
    ASSERT_EQ(0, finished);
    // 模拟快照已落盘但状态推进未完成；原执行者停在 RPC 前，不会并发推进任务。
    ASSERT_EQ(OB_SUCCESS, recovered.switch_status(ObDDLTaskStatus::MODIFY_AUTOINC, true, OB_SUCCESS));
    ASSERT_EQ(ObDDLTaskStatus::MODIFY_AUTOINC, recovered.get_task_status());
    // 消息持久化已移至 MODIFY_AUTOINC，失败后必须保留状态和已有快照。
    {
      class TestIntListItem final : public ObConfigIntListItem
      {
      public:
        explicit TestIntListItem(const char *name)
            : ObConfigIntListItem(nullptr, Scope::TENANT, name, "", "") {}
        const char *value_default_ptr() const override { return ""; }
      };
      TestIntListItem random("offline_test_random");
      TestIntListItem fixed("offline_test_fixed");
      TestIntListItem empty("offline_test_empty");
      ObSqlString point;
      ASSERT_EQ(OB_SUCCESS, point.assign_fmt("%ld",
          static_cast<int64_t>(UPDATE_TASK_RECORD_ON_MESSAGE_FAILED)));
      ASSERT_TRUE(fixed.set_value(point.ptr()));
      auto &sim_mgr = ObDDLSimPointMgr::get_instance();
      const ObDDLSimPointMgr::TaskSimPoint sim_key(
          tenant_id_, recovered.task_id_, UPDATE_TASK_RECORD_ON_MESSAGE_FAILED);
      DEFER({
        EXPECT_EQ(OB_SUCCESS, sim_mgr.set_tenant_param(tenant_id_, empty, empty));
        // 清空 fixed list 不会撤销已经生成的任务注入记录，必须精确删除该记录。
        const int erase_ret = sim_mgr.task_sim_map_.erase_refactored(sim_key);
        EXPECT_TRUE(OB_SUCCESS == erase_ret || OB_HASH_NOT_EXIST == erase_ret) << erase_ret;
      });
      ASSERT_EQ(OB_SUCCESS, sim_mgr.set_tenant_param(tenant_id_, random, fixed));
      // Retryable persistence errors are consumed by switch_status without advancing the task.
      ASSERT_EQ(OB_SUCCESS, recovered.modify_autoinc(ObDDLTaskStatus::TAKE_EFFECT));
      ASSERT_EQ(0, recovered.err_code_occurence_cnt_);
      ASSERT_EQ(ObDDLTaskStatus::MODIFY_AUTOINC, recovered.get_task_status());
      ASSERT_EQ(refresh_snapshot, recovered.alter_table_arg_.async_index_offline_refresh_snapshot_);
      ObDDLTaskRecord failed_record;
      ASSERT_EQ(OB_SUCCESS, ObDDLTaskRecordOperator::get_ddl_task_record(
          tenant_id_, recovered.task_id_, *GCTX.sql_proxy_, allocator, failed_record));
      ObTableRedefinitionTask failed_recovery;
      ASSERT_EQ(OB_SUCCESS, failed_recovery.init(failed_record));
      ASSERT_EQ(ObDDLTaskStatus::MODIFY_AUTOINC, failed_recovery.get_task_status());
      ASSERT_EQ(refresh_snapshot, failed_recovery.alter_table_arg_.async_index_offline_refresh_snapshot_);
    }
    ASSERT_EQ(OB_SUCCESS, DDL_SIM(
        tenant_id_, recovered.task_id_, UPDATE_TASK_RECORD_ON_MESSAGE_FAILED));
    ASSERT_EQ(OB_SUCCESS, recovered.modify_autoinc(ObDDLTaskStatus::TAKE_EFFECT));
    ASSERT_EQ(ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH, recovered.get_task_status());
    ASSERT_EQ(refresh_snapshot, recovered.alter_table_arg_.async_index_offline_refresh_snapshot_);
    ObDDLTaskRecord retry_record;
    ASSERT_EQ(OB_SUCCESS, ObDDLTaskRecordOperator::get_ddl_task_record(
        tenant_id_, recovered.task_id_, *GCTX.sql_proxy_, allocator, retry_record));
    ObTableRedefinitionTask retry_recovery;
    ASSERT_EQ(OB_SUCCESS, retry_recovery.init(retry_record));
    ASSERT_EQ(ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH, retry_recovery.get_task_status());
    ASSERT_EQ(refresh_snapshot, retry_recovery.alter_table_arg_.async_index_offline_refresh_snapshot_);
  } else if (1 == phase) {
    ASSERT_GT(finished, 0);
    ASSERT_LT(finished, physical_before.count());
  } else {
    ASSERT_EQ(physical_before.count(), finished);
  }

  if (phase < 2) {
    if (0 == phase) {
      // 刷新暂停期间，另表 DDL 应能完成；设有界等待，失败时由 DEFER 解锁并回收线程。
      unrelated_thread = std::thread([&]() {
        unrelated_ret.store(exe_sql("alter table t_offline_stage_unrelated add column extra int"));
        unrelated_done.store(true);
      });
      const int64_t deadline = ObTimeUtility::current_time() + 10_s;
      while (!unrelated_done.load() && ObTimeUtility::current_time() < deadline) {
        ob_usleep(10 * 1000);
      }
      ASSERT_TRUE(unrelated_done.load()) << "unrelated DDL blocked by offline preparation";
      unrelated_thread.join();
      ASSERT_EQ(OB_SUCCESS, unrelated_ret.load());
      ObSEArray<int64_t, 8> unchanged;
      ObSEArray<int64_t, 8> unchanged_logical;
      ObSEArray<int64_t, 8> unchanged_commit;
      ASSERT_NO_FATAL_FAILURE(read_offline_versions(
          recovered.target_object_id_, unchanged, unchanged_logical, unchanged_commit));
      ASSERT_EQ(physical_before.count(), unchanged.count());
      for (int64_t i = 0; i < unchanged.count(); ++i) {
        ASSERT_EQ(physical_before.at(i), unchanged.at(i));
        ASSERT_EQ(logical_before.at(i), unchanged_logical.at(i));
        ASSERT_EQ(commit_before.at(i), unchanged_commit.at(i));
      }
    }
    ASSERT_EQ(OB_SUCCESS, recovered.trigger_async_index_refresh());
    ASSERT_EQ(ObDDLTaskStatus::TAKE_EFFECT, recovered.get_task_status());
  } else {
    // 模拟恢复时只看到已提交 swap，不依赖原执行者内存中的成功返回值。
    ASSERT_EQ(OB_SUCCESS, recovered.take_effect(ObDDLTaskStatus::SUCCESS));
    ASSERT_EQ(ObDDLTaskStatus::SUCCESS, recovered.get_task_status());
  }
  ASSERT_EQ(refresh_snapshot, recovered.alter_table_arg_.async_index_offline_refresh_snapshot_);
  ObSEArray<int64_t, 8> physical_after;
  ObSEArray<int64_t, 8> logical_after;
  ObSEArray<int64_t, 8> commit_after;
  ASSERT_NO_FATAL_FAILURE(read_offline_versions(
      recovered.target_object_id_, physical_after, logical_after, commit_after));
  ASSERT_EQ(physical_before.count(), physical_after.count());
  for (int64_t i = 0; i < physical_after.count(); ++i) {
    ASSERT_GE(physical_after.at(i), physical_before.at(i));
    ASSERT_LE(physical_after.at(i), refresh_snapshot);
    ASSERT_EQ(phase < 2 ? refresh_snapshot : commit_before.at(i), logical_after.at(i));
    if (2 == phase) {
      ASSERT_EQ(commit_before.at(i), commit_after.at(i));
    }
  }
  if (2 == phase) {
    // Verify the real swap registered every data partition in the same transaction as the indexes.
    const int64_t publish_version = logical_after.at(0);
    ASSERT_GT(publish_version, refresh_snapshot);
    for (int64_t i = 0; i < logical_after.count(); ++i) {
      ASSERT_EQ(publish_version, logical_after.at(i));
      ASSERT_EQ(publish_version, commit_after.at(i));
    }
    ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
    ObSEArray<ObTabletID, 4> data_tablets;
    ObSEArray<ObLSID, 4> data_ls_ids;
    ASSERT_EQ(OB_SUCCESS, ObDDLUtil::get_tablets(
        tenant_id_, recovered.target_object_id_, data_tablets));
    ASSERT_EQ(2, data_tablets.count());
    ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get_ls(
        *GCTX.sql_proxy_, tenant_id_, data_tablets, data_ls_ids));
    ASSERT_EQ(data_tablets.count(), data_ls_ids.count());
    for (int64_t i = 0; i < data_tablets.count(); ++i) {
      SCOPED_TRACE(data_tablets.at(i).id());
      ObTabletHandle handle;
      ASSERT_EQ(OB_SUCCESS, get_tablet_handle(data_ls_ids.at(i), data_tablets.at(i), handle));
      ObTabletAsyncIndexRefreshInfo info;
      mds::MdsWriter writer;
      mds::TwoPhaseCommitState state;
      SCN commit_version;
      ASSERT_EQ(OB_SUCCESS, handle.get_obj()->mds_get_row_(
          mds::DummyKey(), SCN::max_scn(), 1_s, false /* read_uncommitted */,
          info, nullptr, writer, state, commit_version));
      ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, state);
      ASSERT_TRUE(info.is_valid());
      ASSERT_TRUE(info.has_async_index());
      ASSERT_EQ(2, info.get_refresh_snapshots().count());
      ASSERT_EQ(refresh_snapshot, info.get_refresh_snapshots().at(0));
      ASSERT_EQ(publish_version, info.get_refresh_snapshots().at(1));
      ASSERT_EQ(publish_version, commit_version.get_val_for_tx());
    }
  }
  // 再读落盘记录，不能仅以 recovered 内存中的状态判断恢复成功。
  ObDDLTaskRecord persisted;
  ASSERT_EQ(OB_SUCCESS, ObDDLTaskRecordOperator::get_ddl_task_record(
      tenant_id_, recovered.task_id_, *GCTX.sql_proxy_, allocator, persisted));
  ObTableRedefinitionTask reread;
  ASSERT_EQ(OB_SUCCESS, reread.init(persisted));
  ASSERT_EQ(recovered.get_task_status(), reread.get_task_status());
  ASSERT_EQ(refresh_snapshot, reread.alter_table_arg_.async_index_offline_refresh_snapshot_);

  ASSERT_EQ(OB_SUCCESS, sys_sql("set ob_global_debug_sync='now signal offline_stage_resume'"));
  ddl_thread.join();
  ASSERT_EQ(OB_SUCCESS, ddl_ret.load());
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(proxy,
      "select count(*) as val from hybrid_search(TABLE t_offline_stage_recovery, "
      "'{\"query\":{\"match\":{\"body\":\"recovery\"}}}')", original_id));
  ASSERT_EQ(2, original_id);
}

TEST_F(ObAsyncIndexRefreshScnTest, offline_refresh_recovers_persisted_snapshot_before_rpc)
{
  ASSERT_NO_FATAL_FAILURE(check_offline_refresh_recovery(0));
}

TEST_F(ObAsyncIndexRefreshScnTest, offline_refresh_message_failure_reaches_fail)
{
  ASSERT_NO_FATAL_FAILURE(check_offline_refresh_recovery(3));
}

TEST_F(ObAsyncIndexRefreshScnTest, offline_refresh_recovers_after_first_partition)
{
  ASSERT_NO_FATAL_FAILURE(check_offline_refresh_recovery(1));
}

TEST_F(ObAsyncIndexRefreshScnTest, offline_refresh_recovers_committed_swap)
{
  ASSERT_NO_FATAL_FAILURE(check_offline_refresh_recovery(2));
}

// 使用已有测试专用 mock version 接口；不改持久化版本文件，结束前恢复。
// 暂停在 TAKE_EFFECT 入口，从任务表检查是否已执行前置刷新。
void ObAsyncIndexRefreshScnTest::check_offline_refresh_route(
    const uint64_t version, const bool with_async_index)
{
  using namespace rootserver;
  auto &proxy = get_curr_simple_server().get_sql_proxy2();
  const auto sys_sql = [&](const char *sql) {
    int64_t affected = 0;
    return exec_write_sql_sys(sql, affected);
  };
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table if exists t_offline_version_route"));
  ASSERT_EQ(OB_SUCCESS, exe_sql(with_async_index
      ? "create table t_offline_version_route (id int primary key, body varchar(256), "
        "async fulltext index ft(body)) organization heap"
      : "create table t_offline_version_route (id int primary key, body varchar(256)) organization heap"));
  if (with_async_index) {
    ASSERT_EQ(OB_SUCCESS, exe_sql(
        "alter table t_offline_version_route async_index_params='ASYNC_REFRESH_INTERVAL=-1'"));
  }
  ASSERT_EQ(OB_SUCCESS, exe_sql("insert into t_offline_version_route values (1, 'route token')"));
  ObSqlString sql;
  int64_t table_id = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select table_id as val from oceanbase.__all_virtual_table where tenant_id=%lu "
      "and table_name='t_offline_version_route' and data_table_id=0", tenant_id_));
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(proxy, sql.ptr(), table_id));
  const uint64_t previous_mock_version = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  auto &cluster_version = ObClusterVersion::get_instance();
  const uint64_t previous_data_version = ATOMIC_LOAD(&cluster_version.data_version_);
  const int64_t previous_sync_timeout = static_cast<int64_t>(GCONF.debug_sync_timeout);
  std::thread ddl_thread;
  std::atomic<int> ddl_ret(OB_EAGAIN);
  std::atomic<bool> ddl_done(false);
  DEFER({
    EXPECT_EQ(OB_SUCCESS, sys_sql("set ob_global_debug_sync='now signal offline_route_resume'"));
    EXPECT_EQ(OB_SUCCESS, sys_sql("set ob_global_debug_sync='reset'"));
    if (ddl_thread.joinable()) { ddl_thread.join(); }
    ODV_MGR.set_mock_data_version(previous_mock_version);
    ATOMIC_STORE(&cluster_version.data_version_, previous_data_version);
    ObSqlString restore_sql;
    if (OB_SUCCESS == restore_sql.assign_fmt(
        "alter system set debug_sync_timeout='%ldus'", previous_sync_timeout)) {
      EXPECT_EQ(OB_SUCCESS, sys_sql(restore_sql.ptr()));
    }
    EXPECT_EQ(OB_SUCCESS, exe_sql("drop table if exists t_offline_version_route"));
  });
  ASSERT_EQ(OB_SUCCESS, sys_sql("alter system set debug_sync_timeout='120s'"));
  const int64_t config_deadline = ObTimeUtility::current_time() + 10_s;
  while (120_s != static_cast<int64_t>(GCONF.debug_sync_timeout)
         && ObTimeUtility::current_time() < config_deadline) {
    ob_usleep(10 * 1000);
  }
  ASSERT_EQ(120_s, static_cast<int64_t>(GCONF.debug_sync_timeout));
  ASSERT_EQ(OB_SUCCESS, sys_sql("set ob_global_debug_sync='reset'"));
  ASSERT_EQ(OB_SUCCESS, sys_sql("set ob_global_debug_sync="
      "'BEFORE_TABLE_REDEFINITION_TASK_EFFECT signal offline_route_paused "
      "wait_for offline_route_resume execute 1'"));
  // simple_server 的非零 data_version_ 会让 getter 返回 cluster_version_，
  // 临时改走 ODV_MGR 的测试接口，不改真实 cluster version 或版本文件。
  ATOMIC_STORE(&cluster_version.data_version_, 0);
  ODV_MGR.set_mock_data_version(version);
  uint64_t effective_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(tenant_id_, effective_version));
  ASSERT_EQ(version, effective_version);
  ddl_thread = std::thread([&]() {
    ddl_ret.store(exe_sql("alter table t_offline_version_route "
                         "convert to character set utf8mb4 collate utf8mb4_bin"));
    ddl_done.store(true);
  });
  const int64_t start = ObTimeUtility::current_time();
  // 直接在测试线程等待，避免 sys 租户 SQL 的短超时先于同步点超时生效。
  ObMalloc sync_allocator;
  ObDSSessionActions control_actions;
  ASSERT_EQ(OB_SUCCESS, control_actions.init(1024, sync_allocator));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync(
      "now wait_for offline_route_paused timeout 120000000 execute 1",
      false /* is_global */, control_actions));
  ASSERT_LT(ObTimeUtility::current_time() - start, 110_s);
  ASSERT_FALSE(ddl_done.load());
  ObArenaAllocator allocator;
  ObSEArray<ObDDLTaskRecord, 4> records;
  ASSERT_EQ(OB_SUCCESS, ObDDLTaskRecordOperator::get_ddl_task_record_by_table_id(
      tenant_id_, table_id, *GCTX.sql_proxy_, allocator, records));
  ASSERT_EQ(1, records.count());
  ASSERT_EQ(ObDDLTaskStatus::TAKE_EFFECT, records.at(0).task_status_);
  ObTableRedefinitionTask task;
  ASSERT_EQ(OB_SUCCESS, task.init(records.at(0)));
  if (version >= DATA_VERSION_4_6_3_0 && with_async_index) {
    ASSERT_GT(task.alter_table_arg_.async_index_offline_refresh_snapshot_, 0);
  } else {
    ASSERT_EQ(0, task.alter_table_arg_.async_index_offline_refresh_snapshot_);
  }
  ASSERT_EQ(OB_SUCCESS, sys_sql("set ob_global_debug_sync='now signal offline_route_resume'"));
  ddl_thread.join();
  ASSERT_EQ(OB_SUCCESS, ddl_ret.load());
  if (with_async_index && version >= DATA_VERSION_4_6_3_0) {
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(proxy,
        "select count(*) as val from hybrid_search(TABLE t_offline_version_route, "
        "'{\"query\":{\"match\":{\"body\":\"route\"}}}')", count));
    ASSERT_EQ(1, count);
  }
}

TEST_F(ObAsyncIndexRefreshScnTest, offline_refresh_version_and_membership_routes)
{
  const uint64_t versions[] = {DATA_VERSION_4_6_2_1, DATA_VERSION_4_6_3_0};
  for (const uint64_t version : versions) {
    SCOPED_TRACE(version);
    if (version >= DATA_VERSION_4_6_3_0) {
      ASSERT_NO_FATAL_FAILURE(check_offline_refresh_route(version, false));
    }
    ASSERT_NO_FATAL_FAILURE(check_offline_refresh_route(version, true));
  }
}
// Exercise both SQL entry points, rather than constructing refresh_info with S
// already installed. A zero initial_snapshot regression would retain C instead.
void ObAsyncIndexRefreshScnTest::check_first_online_index_snapshot(const bool use_alter)
{
  using namespace rootserver;
  SCOPED_TRACE(use_alter ? "ALTER ADD" : "CREATE INDEX");
  auto &proxy = get_curr_simple_server().get_sql_proxy2();
  const auto sys_sql = [&](const char *sql) {
    int64_t affected = 0;
    return exec_write_sql_sys(sql, affected);
  };
  const uint64_t previous_mock_version = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  auto &cluster_version = ObClusterVersion::get_instance();
  const uint64_t previous_data_version = ATOMIC_LOAD(&cluster_version.data_version_);
  const int64_t previous_sync_timeout = static_cast<int64_t>(GCONF.debug_sync_timeout);
  ObMalloc sync_allocator;
  ObDSSessionActions control_actions;
  ASSERT_EQ(OB_SUCCESS, control_actions.init(1024, sync_allocator));
  // Fresh events also isolate CREATE/ALTER and repeated gtest runs from signals
  // emitted by the cleanup path after a waiter has already consumed its signal.
  // ObSyncEventName has a 32-byte buffer; keep prefixes short enough for the timestamp.
  const int64_t event_id = ObTimeUtility::current_time();
  ObSqlString pause_action;
  ObSqlString wait_action;
  ObSqlString resume_action;
  ObSqlString ddl_done_action;
  ASSERT_EQ(OB_SUCCESS, pause_action.assign_fmt(
      "set ob_global_debug_sync='BUILD_FTS_INDEX_PREPARE_STATUS signal fs_hit_%ld "
      "wait_for fs_go_%ld timeout 120000000 execute 1'", event_id, event_id));
  ASSERT_EQ(OB_SUCCESS, wait_action.assign_fmt(
      "now wait_for fs_hit_%ld timeout 60000000 execute 1", event_id));
  ASSERT_EQ(OB_SUCCESS, resume_action.assign_fmt("now signal fs_go_%ld", event_id));
  ASSERT_EQ(OB_SUCCESS, ddl_done_action.assign_fmt("now signal fs_hit_%ld", event_id));
  std::thread ddl_thread;
  std::atomic<int> ddl_ret(OB_EAGAIN);
  std::atomic<bool> ddl_done(false);
  DEFER({
    // Release even after a fatal assertion, before joining or restoring versions.
    EXPECT_EQ(OB_SUCCESS, sys_sql(
        "set ob_global_debug_sync='BUILD_FTS_INDEX_PREPARE_STATUS clear'"));
    EXPECT_EQ(OB_SUCCESS, GDS.add_debug_sync(
        resume_action.string(), false, control_actions));
    if (ddl_thread.joinable()) { ddl_thread.join(); }
    ODV_MGR.set_mock_data_version(previous_mock_version);
    ATOMIC_STORE(&cluster_version.data_version_, previous_data_version);
    ObSqlString restore_sql;
    if (OB_SUCCESS == restore_sql.assign_fmt(
        "alter system set debug_sync_timeout='%ldus'", previous_sync_timeout)) {
      EXPECT_EQ(OB_SUCCESS, sys_sql(restore_sql.ptr()));
    }
    EXPECT_EQ(OB_SUCCESS, GDS.add_debug_sync("reset", false, control_actions));
    EXPECT_EQ(OB_SUCCESS, exe_sql("drop table if exists t_first_online_snapshot"));
  });
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table if exists t_first_online_snapshot"));
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "create table t_first_online_snapshot (id int primary key, body varchar(256)) organization heap"));
  // Nonempty input prevents the empty-table optimization from bypassing PREPARE.
  ASSERT_EQ(OB_SUCCESS, exe_sql("insert into t_first_online_snapshot values (1, 'snapshot token')"));
  int64_t table_id = 0;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select table_id as val from oceanbase.__all_virtual_table where tenant_id=%lu "
      "and table_name='t_first_online_snapshot' and data_table_id=0", tenant_id_));
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(proxy, sql.ptr(), table_id));
  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(proxy, table_id, tablet_id, ls_id));
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  ObTabletHandle tablet;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_id, tablet_id, tablet));
  bool has_async_index = true;
  ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->get_has_async_index(has_async_index));
  ASSERT_FALSE(has_async_index);
  // CREATE/ALTER require refresh to be enabled. Quiesce only the periodic
  // scheduler, leaving the DDL-driven worker available after PREPARE resumes.
  const int64_t refresh_deadline = ObTimeUtility::current_time() + 10_s;
  while (!ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(tenant_id_)
         && ObTimeUtility::current_time() < refresh_deadline) {
    ob_usleep(10 * 1000);
  }
  ASSERT_TRUE(ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(tenant_id_));
  ObAsyncIndexService *service = MTL(ObAsyncIndexService *);
  ASSERT_NE(nullptr, service);
  ObTenantAsyncIndexRefresher &refresher = service->get_refresher();
  ASSERT_EQ(OB_SUCCESS, TG_CANCEL_R(refresher.tg_id_, refresher.timer_task_));
  DEFER(EXPECT_EQ(OB_SUCCESS, TG_SCHEDULE(refresher.tg_id_, refresher.timer_task_,
      ObTenantAsyncIndexRefresher::SCHEDULE_PERIOD_US, true /*repeat*/)));
  ASSERT_EQ(OB_SUCCESS, TG_INVOKE(refresher.tg_id_, wait_task, refresher.timer_task_));
  ASSERT_EQ(OB_SUCCESS, sys_sql("alter system set debug_sync_timeout='120s'"));
  const int64_t config_deadline = ObTimeUtility::current_time() + 10_s;
  while (120_s != static_cast<int64_t>(GCONF.debug_sync_timeout)
         && ObTimeUtility::current_time() < config_deadline) {
    ob_usleep(10 * 1000);
  }
  ASSERT_EQ(120_s, static_cast<int64_t>(GCONF.debug_sync_timeout));
  ASSERT_EQ(OB_SUCCESS, sys_sql(pause_action.ptr()));
  // 4.6.3 defers S to PREPARE. Use the supported older protocol to exercise
  // new_fetched_snapshot -> initial_snapshot in the CREATE/ALTER transaction.
  ATOMIC_STORE(&cluster_version.data_version_, 0);
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_2_1);
  uint64_t effective_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(tenant_id_, effective_version));
  ASSERT_EQ(DATA_VERSION_4_6_2_1, effective_version);
  ddl_thread = std::thread([&]() {
    ddl_ret.store(exe_sql(use_alter
        ? "alter table t_first_online_snapshot add async fulltext index ft(body)"
        : "create async fulltext index ft on t_first_online_snapshot(body)"));
    ddl_done.store(true);
    // Wake the controller on early SQL failure as well as on the sync point.
    // Use thread-local actions: control_actions belongs to the waiting thread.
    ObMalloc allocator;
    ObDSSessionActions actions;
    if (OB_SUCCESS == actions.init(1024, allocator)) {
      GDS.add_debug_sync(ddl_done_action.string(), false, actions);
    }
  });
  const int64_t wait_started = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync(
      wait_action.string(), false, control_actions));
  if (ddl_done.load()) {
    ASSERT_EQ(OB_SUCCESS, ddl_ret.load()) << "DDL exited before PREPARE";
    FAIL() << "DDL completed without pausing at PREPARE";
  }
  // DEBUG_SYNC timeout returns success; verify that the handshake actually hit.
  ASSERT_LT(ObTimeUtility::current_time() - wait_started, 59_s);
  ASSERT_FALSE(ddl_done.load());
  ObArenaAllocator allocator;
  ObSEArray<ObDDLTaskRecord, 4> records;
  ASSERT_EQ(OB_SUCCESS, ObDDLTaskRecordOperator::get_ddl_task_record_by_table_id(
      tenant_id_, table_id, *GCTX.sql_proxy_, allocator, records));
  ASSERT_EQ(1, records.count());
  ASSERT_EQ(ObDDLTaskStatus::PREPARE, records.at(0).task_status_);
  const int64_t selected_snapshot = records.at(0).snapshot_version_;
  ASSERT_GT(selected_snapshot, 0);
  // PREPARE has not started the build/DDL refresh; the periodic timer is quiesced.
  // Read C independently from the committed membership MDS row, not from S.
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_id, tablet_id, tablet));
  ObTabletAsyncIndexRefreshInfo info;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState state;
  SCN commit_version;
  ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->get_latest_async_index_refresh_info(
      info, writer, state, commit_version));
  ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, state);
  ASSERT_TRUE(info.has_async_index());
  ASSERT_LT(selected_snapshot, commit_version.get_val_for_tx());
  ASSERT_EQ(1, info.get_refresh_snapshots().count());
  ASSERT_EQ(selected_snapshot, info.get_refresh_snapshots().at(0));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync(
      resume_action.string(), false, control_actions));
  ddl_thread.join();
  ASSERT_EQ(OB_SUCCESS, ddl_ret.load());
}

TEST_F(ObAsyncIndexRefreshScnTest, create_first_online_index_preserves_selected_snapshot)
{
  ASSERT_NO_FATAL_FAILURE(check_first_online_index_snapshot(false));
}

TEST_F(ObAsyncIndexRefreshScnTest, alter_first_online_index_preserves_selected_snapshot)
{
  ASSERT_NO_FATAL_FAILURE(check_first_online_index_snapshot(true));
}
#endif // ERRSIM

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
