/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <sstream>
#define private public
#define protected public
#include "simple_server/env/ob_simple_cluster_test_base.h"
#include "simple_server/env/ob_simple_server_restart_helper.h"
#include "unittest/storage/test_tablet_helper.h"
#include "unittest/storage/init_basic_struct.h"
#include "unittest/storage/mock_ob_log_handler.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "observer/ob_rpc_processor_simple.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/ddl/ob_ss_ddl_util.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/compaction_v2/ob_ss_compact_helper.h"
#include "storage/compaction_v2/ob_ss_link_major_task.h"
#include "storage/tablet/ob_tablet_persist_common.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/mds_table/ob_mds_table.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
char *shared_storage_info = nullptr;
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace share;
using namespace share::schema;

namespace
{
class FailingFinishLogHandler : public MockObLogHandler
{
public:
  explicit FailingFinishLogHandler(const int error_code)
    : error_code_(error_code), append_count_(0)
  {}
  int append(const void *buffer, const int64_t nbytes, const SCN &ref_scn,
      const bool need_nonblock, const bool allow_compress, logservice::AppendCb *cb,
      palf::LSN &lsn, SCN &scn) override
  {
    UNUSEDx(buffer, nbytes, ref_scn, need_nonblock, allow_compress, cb, lsn, scn);
    ++append_count_;
    return error_code_;
  }
  int64_t get_append_count() const { return append_count_; }
private:
  int error_code_;
  int64_t append_count_;
};

class ScopedLinkFailure
{
public:
  ScopedLinkFailure() : previous_(active_) { active_ = true; }
  ~ScopedLinkFailure() { active_ = previous_; }
  static bool is_active() { return active_; }
private:
  static thread_local bool active_;
  bool previous_;
  DISALLOW_COPY_AND_ASSIGN(ScopedLinkFailure);
};
thread_local bool ScopedLinkFailure::active_ = false;
}

namespace share
{
int SSCompactHelper::link_major(const ObLSID &ls_id, const ObTabletID &tablet_id,
    const compaction::ObMergeType merge_type, const int64_t snapshot_version,
    const int64_t merge_base_version, const bool prewarm, const bool report)
{
  int ret = OB_SUCCESS;
  if (ScopedLinkFailure::is_active()) {
    ret = OB_TIMEOUT;
  } else {
    compaction::ObTabletSSLinkMajorTask task(ls_id, tablet_id, snapshot_version,
        merge_base_version, merge_type, prewarm, report);
    ret = task.process();
  }
  return ret;
}
}

namespace unittest
{
class TestDDLFinishRecovery : public ObSimpleClusterTestBase
{
public:
  TestDDLFinishRecovery()
    : ObSimpleClusterTestBase("test_ddl_finish_recovery", "10G", "20G", "100G"),
      allocator_(ObMemAttr(OB_SYS_TENANT_ID, "DDLFinishTest"))
  {}
  void prepare_test_tenant();
  void prepare_published_finish(const ObTabletID &tablet_id, ObDDLFinishLog &finish_log,
      ObDDLKVHandle &kv_handle);
  void make_finish_log(const ObSSTable &sstable, ObDDLFinishLog &finish_log);
  void fail_finish_append(const ObTabletID &tablet_id, const int append_error);
  void check_durable_major(const ObTabletID &tablet_id);

  static uint64_t test_tenant_id_;
  static constexpr int64_t TEST_LS_ID = 1001;
  static constexpr int64_t SNAPSHOT_VERSION = 100;
  static constexpr uint64_t BASE_TABLET_ID = 900057996;
  ObArenaAllocator allocator_;
};

uint64_t TestDDLFinishRecovery::test_tenant_id_ = OB_INVALID_ID;

void TestDDLFinishRecovery::prepare_test_tenant()
{
  if (!ObSimpleServerRestartHelper::is_restart_ && OB_INVALID_ID == test_tenant_id_) {
    ASSERT_EQ(OB_SUCCESS, create_tenant_with_retry("tt1", "4G", "4G", false, 2, "16G"));

    // Finish bootstrap metadata checkpointing before creating the DDL failure window.
    ObTenantSwitchGuard sys_guard;
    ASSERT_EQ(OB_SUCCESS, sys_guard.switch_to(OB_SYS_TENANT_ID, false));
    ObLSHandle sys_ls;
    SCN end_scn;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
        ObLSID(1), sys_ls, ObLSGetMod::DDL_MOD));
    ASSERT_EQ(OB_SUCCESS, sys_ls.get_ls()->get_end_scn(end_scn));
    const int64_t deadline = ObTimeUtility::current_time() + 120 * 1000 * 1000L;
    while (sys_ls.get_ls()->get_ls_meta().get_clog_checkpoint_scn() < end_scn
        && ObTimeUtility::current_time() < deadline) {
      ASSERT_EQ(OB_SUCCESS, sys_ls.get_ls()->advance_checkpoint_by_flush(
          SCN::max_scn(), INT64_MAX, false, ObFreezeSourceFlag::TEST_MODE));
      ob_usleep(500 * 1000L);
    }
    ASSERT_GE(sys_ls.get_ls()->get_ls_meta().get_clog_checkpoint_scn(), end_scn);
  }
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(test_tenant_id_, "tt1"));
  ASSERT_TRUE(is_user_tenant(test_tenant_id_));
  allocator_.set_tenant_id(test_tenant_id_);
}

void TestDDLFinishRecovery::make_finish_log(
    const ObSSTable &sstable, ObDDLFinishLog &finish_log)
{
  ObSSTablePersistWrapper persister(DATA_CURRENT_VERSION, &sstable);
  const int64_t buf_len = persister.get_serialize_size();
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  int64_t pos = 0;
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, persister.serialize(buf, buf_len, pos));
  ASSERT_EQ(OB_SUCCESS, finish_log.init(test_tenant_id_, ObLSID(TEST_LS_ID), sstable.get_key(),
      buf, pos, DATA_CURRENT_VERSION));
}

void TestDDLFinishRecovery::prepare_published_finish(
    const ObTabletID &tablet_id, ObDDLFinishLog &finish_log, ObDDLKVHandle &kv_handle)
{
  ASSERT_TRUE(GCTX.is_shared_storage_mode());
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTableSchema schema;
  ObStorageSchema storage_schema;
  ObTabletCreateSSTableParam param;
  ObSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
      ObLSID(TEST_LS_ID), ls_handle, ObLSGetMod::DDL_MOD));
  ASSERT_EQ(OB_SUCCESS, build_test_schema(schema, tablet_id.id(), "ddl_finish_recovery"));
  schema.set_tenant_id(test_tenant_id_);
  schema.set_table_type(USER_TABLE);
  schema.set_rowkey_column_num(1);
  schema.set_max_used_column_id(16);
  ObTableMode mode = schema.get_table_mode_struct();
  mode.state_flag_ = TABLE_STATE_IS_HIDDEN_MASK;
  schema.set_table_mode_struct(mode);
  SCN create_scn;
  bool external_consistent = false;
  ASSERT_EQ(OB_SUCCESS, OB_TS_MGR.get_ts_sync(
      test_tenant_id_, 10 * 1000 * 1000L, create_scn, external_consistent));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, schema,
      allocator_, ObTabletStatus::NORMAL, create_scn, tablet_handle,
      TestTabletHelper::StatusInjectMode::MDS_REPLAY));
  // The helper injects CREATE through replay; persist it before testing a crash after LINK.
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->flush_mds_table());
  SCN mds_checkpoint;
  const int64_t deadline = ObTimeUtility::current_time() + 60 * 1000 * 1000L;
  do {
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_mds_table()->get_mds_checkpoint_scn(
        mds_checkpoint));
    if (mds_checkpoint < create_scn) {
      ob_usleep(100 * 1000L);
    }
  } while (mds_checkpoint < create_scn && ObTimeUtility::current_time() < deadline);
  ASSERT_GE(mds_checkpoint, create_scn);
  ASSERT_EQ(0, tablet_handle.get_obj()->get_major_table_count());
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, schema, lib::Worker::CompatMode::MYSQL,
      false /*skip_column_info*/, DATA_CURRENT_VERSION, false /*generate_cs_replica_cg_array*/));
  ASSERT_EQ(OB_SUCCESS, param.init_for_empty_major_sstable(tablet_id, storage_schema,
      SNAPSHOT_VERSION, -1 /*column_group_idx*/, false /*has_all_column_group*/,
      true /*is_shared*/));
  ASSERT_EQ(OB_SUCCESS, sstable.init(param, &allocator_));
  ASSERT_EQ(OB_SUCCESS, ObSSDDLUtil::update_shared_tablet_table_store(
      ls_handle, sstable, storage_schema, DATA_CURRENT_VERSION,
      tablet_handle.get_obj()->get_reorganization_scn(), false /*check_lease*/,
      SCN::min_scn(), false /*need_check_ddl_checkpoint*/));
  ASSERT_EQ(0, tablet_handle.get_obj()->get_major_table_count());
  ASSERT_NO_FATAL_FAILURE(make_finish_log(sstable, finish_log));

  ObDDLKvMgrHandle kv_mgr;
  SCN redo_scn;
  SCN start_scn;
  ASSERT_EQ(OB_SUCCESS, redo_scn.convert_for_tx(101));
  ASSERT_EQ(OB_SUCCESS, start_scn.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_ddl_kv_mgr(kv_mgr, true /*try_create*/));
  ASSERT_EQ(OB_SUCCESS, kv_mgr.get_obj()->get_or_create_idem_ddl_kv(
      redo_scn, start_scn, SNAPSHOT_VERSION, DATA_CURRENT_VERSION, kv_handle));
  ASSERT_FALSE(kv_handle.get_obj()->is_freezed());
}

void TestDDLFinishRecovery::check_durable_major(const ObTabletID &tablet_id)
{
  ASSERT_TRUE(GCTX.is_shared_storage_mode());
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObArenaAllocator reload_allocator("DDLFinishReload", OB_MALLOC_NORMAL_BLOCK_SIZE,
      test_tenant_id_, ObCtxIds::DEFAULT_CTX_ID);
  ObTabletHandle reloaded_tablet;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
      ObLSID(TEST_LS_ID), ls_handle, ObLSGetMod::DDL_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle,
      ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT, ObMDSGetTabletMode::READ_ALL_COMMITED));
  ASSERT_EQ(1, tablet_handle.get_obj()->get_major_table_count());
  ASSERT_TRUE(tablet_handle.get_obj()->get_tablet_addr().is_disked());
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantMetaMemMgr *)->get_tablet_with_allocator(
      WashTabletPriority::WTP_LOW, ObTabletMapKey(ObLSID(TEST_LS_ID), tablet_id), reload_allocator,
      reloaded_tablet, true /*force_alloc_new*/));
  EXPECT_NE(tablet_handle.get_obj(), reloaded_tablet.get_obj());
  EXPECT_EQ(SNAPSHOT_VERSION, reloaded_tablet.get_obj()->get_last_major_snapshot_version());
  EXPECT_EQ(1, reloaded_tablet.get_obj()->get_major_table_count());
}

void TestDDLFinishRecovery::fail_finish_append(const ObTabletID &tablet_id, const int append_error)
{
  ObDDLFinishLog finish_log;
  ObDDLKVHandle kv_handle;
  ASSERT_NO_FATAL_FAILURE(prepare_published_finish(tablet_id, finish_log, kv_handle));
  FailingFinishLogHandler log_handler(append_error);
  ObDDLRedoLogWriter writer;
  ObDDLFinishLogHandle finish_handle;
  ASSERT_EQ(OB_SUCCESS, writer.init(ObLSID(TEST_LS_ID), tablet_id));
  EXPECT_EQ(append_error, writer.local_write_ddl_finish_log(
      finish_log, ObLSID(TEST_LS_ID), &log_handler, finish_handle));
  ASSERT_EQ(1, log_handler.get_append_count());
  EXPECT_EQ(nullptr, finish_handle.cb_);
  EXPECT_FALSE(kv_handle.get_obj()->is_freezed());
  ASSERT_NO_FATAL_FAILURE(check_durable_major(tablet_id));
}

TEST_F(TestDDLFinishRecovery, before_restart_append_timeout)
{
  // Persist a real MAJOR, reject FINISH before append, and leave it for process recovery.
  ASSERT_NO_FATAL_FAILURE(prepare_test_tenant());
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(test_tenant_id_, false));
  fail_finish_append(ObTabletID(BASE_TABLET_ID), OB_TIMEOUT);
}

TEST_F(TestDDLFinishRecovery, before_restart_remote_link_gate)
{
  // The receiving leader must reject failed LINK and complete a real LINK on retry.
  ASSERT_NO_FATAL_FAILURE(prepare_test_tenant());
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(test_tenant_id_, false));
  const ObTabletID tablet_id(BASE_TABLET_ID + 1);
  ObDDLFinishLog finish_log;
  ObDDLKVHandle kv_handle;
  ASSERT_NO_FATAL_FAILURE(prepare_published_finish(tablet_id, finish_log, kv_handle));
  observer::ObRpcRemoteWriteDDLFinishLogP processor(GCTX);
  ASSERT_EQ(OB_SUCCESS, processor.arg_.init(test_tenant_id_, finish_log.get_log_info()));
  {
    ScopedLinkFailure link_failure;
    EXPECT_EQ(OB_TIMEOUT, processor.process());
    EXPECT_FALSE(kv_handle.get_obj()->is_freezed());
  }
  // Empty KV has no redo SCN to freeze; verify FINISH completion and durable MAJOR.
  ASSERT_EQ(OB_SUCCESS, processor.process());
  ASSERT_NO_FATAL_FAILURE(check_durable_major(tablet_id));
}

TEST_F(TestDDLFinishRecovery, before_restart_append_not_master)
{
  // A role error before append must leave the same recoverable persisted state.
  ASSERT_NO_FATAL_FAILURE(prepare_test_tenant());
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(test_tenant_id_, false));
  fail_finish_append(ObTabletID(BASE_TABLET_ID + 2), OB_NOT_MASTER);
}

TEST_F(TestDDLFinishRecovery, after_restart_completes_missing_finish)
{
  // The restart helper exits the first observer without closing it and starts a new process.
  ASSERT_NO_FATAL_FAILURE(prepare_test_tenant());
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(test_tenant_id_, false));
  const uint64_t tablet_ids[] = {BASE_TABLET_ID, BASE_TABLET_ID + 2};
  for (int64_t i = 0; i < 2; ++i) {
    const ObTabletID tablet_id(tablet_ids[i]);
    ASSERT_NO_FATAL_FAILURE(check_durable_major(tablet_id));
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    ObTabletMemberWrapper<ObTabletTableStore> store;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
        ObLSID(TEST_LS_ID), ls_handle, ObLSGetMod::DDL_MOD));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle,
        ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT, ObMDSGetTabletMode::READ_ALL_COMMITED));
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->fetch_table_store(store));
    const ObSSTable *major = store.get_member()->get_major_sstables().at(0);
    ASSERT_NE(nullptr, major);
    ObDDLFinishLog finish_log;
    ASSERT_NO_FATAL_FAILURE(make_finish_log(*major, finish_log));
    ObDDLRedoLogWriter writer;
    ObDDLFinishLogHandle finish_handle;
    ASSERT_EQ(OB_SUCCESS, writer.init(ObLSID(TEST_LS_ID), tablet_id));
    ASSERT_EQ(OB_SUCCESS, writer.local_write_ddl_finish_log(finish_log, ObLSID(TEST_LS_ID),
        ls_handle.get_ls()->get_log_handler(), finish_handle));
    ASSERT_NE(nullptr, finish_handle.cb_);
    ASSERT_EQ(OB_SUCCESS, finish_handle.wait(10 * 1000 * 1000L));
    ASSERT_NO_FATAL_FAILURE(check_durable_major(tablet_id));
  }
}
}
}

int main(int argc, char **argv)
{
  char storage_info[1000] = {0};
  const int config_ret = oceanbase::common::databuff_printf(storage_info, sizeof(storage_info),
      "%s/%ld?host=%s&access_id=%s&access_key=%s&s3_region=%s"
      "&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, oceanbase::common::ObTimeUtility::current_time_ns(),
      oceanbase::unittest::S3_ENDPOINT, oceanbase::unittest::S3_AK,
      oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  if (OB_SUCCESS != config_ret) {
    return config_ret;
  }
  oceanbase::shared_storage_info = storage_info;
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");
  const int extra_ret = GCONF.add_extra_config("sys_ckpt_thread_score=32");
  if (OB_SUCCESS != extra_ret) {
    return extra_ret;
  }
  ObSimpleServerRestartHelper restart_helper(argc, argv, "test_ddl_finish_recovery",
      "TestDDLFinishRecovery.before_restart", "TestDDLFinishRecovery.after_restart", "WARN");
  return restart_helper.run();
}
