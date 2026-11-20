/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// 该用例验证 shared-storage 模式下 local minor 的"上传水位限制"：
// end_scn 超过 ss tablet 上实际已上传 sstable end_scn 的 mini 不允许参与 local minor，
// 因此在上传注册被阻断时，本地 minor sstable 只会累积、不会被合并下去。
//
// 制造未上传积压的手段：errsim LS_SS_INC_UPLOAD_SKIP_REGISTER 阻断常规增量上传的注册。

#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#define UNITTEST

#include <cstdlib>
#include "lib/checksum/ob_crc64.h"
#include "lib/literals/ob_literals.h"

#include "storage/tx_storage/ob_ls_service.h"
#include "storage/access/ob_lob_column_iterator.h"
#include "storage/backup/ob_ss_backup_utils.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_sstable_private_object_cleaner.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/lob/store/ob_outtable_lob_control_operator.h"
#include "storage/high_availability/ob_ss_storage_ha_writer.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_remote_io_adapter.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "lib/utility/ob_defer.h"
#include "lib/restore/ob_object_storage_base.h"
#include "src/storage/incremental/ob_shared_meta_service.h"
#include "src/storage/shared_storage/ob_dir_manager.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
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
namespace unittest
{

using namespace oceanbase::storage;
using namespace oceanbase::compaction;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_ = 0;
  ObTabletID tablet_id_;
  int64_t time_sec_ = 0;
};

TestRunCtx RunCtx;

class ObSSLocalMinorWatermarkTest : public ObSimpleClusterTestBase
{
public:
  ObSSLocalMinorWatermarkTest()
      : ObSimpleClusterTestBase("test_ss_local_minor_upload_watermark_", "50G", "50G", "50G")
  {}

  void wait_minor_freeze_finish();
  void set_ls_and_tablet_id_for_run_ctx();
  int get_local_minor_table_info(int64_t &minor_cnt, share::SCN &min_end_scn, share::SCN &max_end_scn);
  int get_ss_uploaded_minor_ckpt_scn(share::SCN &uploaded_ckpt_scn, const int64_t max_retry = 300);

  void create_lob_meta_sstable(
      common::ObArenaAllocator &allocator,
      const share::schema::ObTableSchema &meta_table_schema,
      const ObStorageSchema &meta_schema,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObMergeType merge,
      const int64_t object_size,
      const int64_t file_idx,
      const blocksstable::MacroBlockId &reference,
      blocksstable::ObSSTablePrivateObjectCleaner &cleaner,
      ObTableHandleV2 &handle);

  void wait_sys_to_leader()
  {
    share::ObTenantSwitchGuard tenant_guard;
    int ret = OB_ERR_UNEXPECTED;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(1));
    ObLS *ls = nullptr;
    ObLSID ls_id(ObLSID::SYS_LS_ID);
    ObLSHandle handle;
    ObLSService *ls_svr = MTL(ObLSService *);
    EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ls = handle.get_ls();
    ASSERT_NE(nullptr, ls);
    ASSERT_EQ(ls_id, ls->get_ls_id());
    for (int i = 0; i < 100; i++) {
      ObRole role;
      int64_t proposal_id = 0;
      ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
      if (role == ObRole::LEADER) {
        ret = OB_SUCCESS;
        break;
      }
      ob_usleep(10 * 1000);
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }
};

#define EXE_SQL(sql_str)                                                                          \
  LOG_INFO("exe sql start", K(sql_str));                                                          \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                                                     \
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows)); \
  LOG_INFO("exe sql end", K(sql_str));

// set_tp（设置 tracepoint）是全局调试操作，需要 SYS 租户权限，必须用 SYS 连接（get_sql_proxy）执行
#define SYS_EXE_SQL(sql_str)                                                                      \
  LOG_INFO("sys exe sql start", K(sql_str));                                                      \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                                                     \
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write(sql.ptr(), affected_rows));\
  LOG_INFO("sys exe sql end", K(sql_str));

// 等待本轮 minor freeze 完成（memtable 全部转成 mini sstable，table_type=0 表示 memtable）
void ObSSLocalMinorWatermarkTest::wait_minor_freeze_finish()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObSqlString sql;
  int64_t row_cnt = 0;
  int64_t retry = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr "
        "where tenant_id=%lu and tablet_id=%lu and table_type=0;",
        RunCtx.tenant_id_, RunCtx.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    usleep(100 * 1000);
  } while (row_cnt > 0 && ++retry < 600);
  ASSERT_EQ(0, row_cnt);
}

void ObSSLocalMinorWatermarkTest::set_ls_and_tablet_id_for_run_ctx()
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  uint64_t uid = 0;
  int64_t id = 0;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, sql.assign(
      "select tablet_id from oceanbase.__all_virtual_table where table_name='test_table';"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_uint("tablet_id", uid));
  }
  RunCtx.tablet_id_ = uid;

  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select ls_id from oceanbase.__all_tablet_to_ls where tablet_id=%ld;", uid));
  SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res2, sql.ptr()));
    sqlclient::ObMySQLResult *result = res2.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", id));
  }
  ObLSID ls_id(id);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  RunCtx.ls_id_ = ls->get_ls_id();
  RunCtx.ls_epoch_ = ls->get_ls_epoch();
  RunCtx.tenant_epoch_ = MTL_EPOCH_ID();
  LOG_INFO("finish set run ctx", K(RunCtx.ls_id_), K(RunCtx.ls_epoch_), K(RunCtx.tablet_id_));
}

int ObSSLocalMinorWatermarkTest::get_local_minor_table_info(
    int64_t &minor_cnt,
    share::SCN &min_end_scn,
    share::SCN &max_end_scn)
{
  int ret = OB_SUCCESS;
  minor_cnt = 0;
  min_end_scn.set_max();
  max_end_scn.set_base();
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTableStoreIterator minor_iter;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(RunCtx.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls fail", K(ret), K(RunCtx.ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(
                 RunCtx.tablet_id_, tablet_handle,
                 ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S,
                 ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get local tablet fail", K(ret), K(RunCtx.tablet_id_));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_all_minor_sstables(minor_iter))) {
    LOG_WARN("get local minor sstables fail", K(ret), K(RunCtx.tablet_id_));
  } else if (!minor_iter.is_valid()) {
    min_end_scn.set_base();
  } else {
    while (OB_SUCC(ret)) {
      ObTableHandleV2 table_handle;
      blocksstable::ObSSTable *sstable = nullptr;
      if (OB_FAIL(minor_iter.get_next(table_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next local minor fail", K(ret), K(RunCtx.tablet_id_));
        }
      } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
        LOG_WARN("get local minor sstable fail", K(ret), K(table_handle));
      } else if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("local minor sstable is null", K(ret), K(table_handle));
      } else {
        ++minor_cnt;
        min_end_scn = MIN(min_end_scn, sstable->get_end_scn());
        max_end_scn = MAX(max_end_scn, sstable->get_end_scn());
      }
    }
    if (0 == minor_cnt) {
      min_end_scn.set_base();
    }
  }
  return ret;
}

int ObSSLocalMinorWatermarkTest::get_ss_uploaded_minor_ckpt_scn(
    share::SCN &uploaded_ckpt_scn,
    const int64_t max_retry)
{
  int ret = OB_SUCCESS;
  uploaded_ckpt_scn.set_base();
  ObLSHandle ls_handle;
  ObTabletHandle local_tablet_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(RunCtx.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls fail", K(ret), K(RunCtx.ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(
                 RunCtx.tablet_id_, local_tablet_handle,
                 ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S,
                 ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get local tablet fail", K(ret), K(RunCtx.tablet_id_));
  } else {
    const share::SCN reorganization_scn = local_tablet_handle.get_obj()->get_reorganization_scn();
    for (int64_t i = 0; i < max_retry; ++i) {
      ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "SSUploadEnd"));
      ObTabletHandle ss_tablet_handle;
      ret = MTL(ObSSMetaService *)->get_tablet(
          RunCtx.ls_id_, RunCtx.tablet_id_, reorganization_scn, allocator, ss_tablet_handle);
      if (OB_SUCCESS == ret && ss_tablet_handle.is_valid()) {
        uploaded_ckpt_scn = ss_tablet_handle.get_obj()->get_clog_checkpoint_scn();
        break;
      } else if (OB_TABLET_NOT_EXIST == ret || (OB_SUCCESS == ret && !ss_tablet_handle.is_valid())) {
        ret = OB_TABLET_NOT_EXIST;
        usleep(100 * 1000);
      } else {
        LOG_WARN("get ss tablet fail", K(ret), K(RunCtx.tablet_id_));
        break;
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("get ss uploaded minor end scn still fail after retry", K(ret), K(RunCtx.tablet_id_));
    }
  }
  return ret;
}

TEST_F(ObSSLocalMinorWatermarkTest, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(ObSSLocalMinorWatermarkTest, add_tenant)
{
  TRANS_LOG(INFO, "create_tenant start");
  wait_sys_to_leader();
  int ret = OB_SUCCESS;
  int retry_cnt = 0;
  do {
    if (OB_FAIL(create_tenant("tt1"))) {
      TRANS_LOG(WARN, "create_tenant fail, need retry", K(ret), K(retry_cnt));
      ob_usleep(15 * 1000 * 1000); // 15s
    }
    retry_cnt++;
  } while (OB_FAIL(ret) && retry_cnt < 10);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());

  int64_t affected_rows = 0;
  ObSqlString sql;
  // 加快上传调度（不阻断时），方便对照
  EXE_SQL("alter system set _shared_inc_sstable_upload_interval = '1s';");
  EXE_SQL("alter system set inc_sstable_upload_thread_score = 20;");
  // 让 minor compact trigger 足够小：若没有水位限制，2 个 mini 就会触发 local minor。
  // 通过 sql_proxy2（tt1 连接）设置当前租户配置，不能带 "tenant tt1" 子句（那是 SYS 级操作）。
  EXE_SQL("alter system set minor_compact_trigger = 2;");
}

// 正常态：end_scn 超过 ss tablet 已上传 sstable end_scn 的本地 minor 不应再次参与 local minor 候选
TEST_F(ObSSLocalMinorWatermarkTest, normal_state_un_uploaded_mini_not_merged)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  ObSqlString sql;
  int64_t affected_rows = 0;

  EXE_SQL("create table test_table (a int)");
  set_ls_and_tablet_id_for_run_ctx();

  // 先不打开 tracepoint，做一次基线 freeze 并等待 ss tablet 首次创建成功。
  // 否则 tracepoint 可能把首次 shared tablet 创建也拦住，导致本用例和后台上传线程抢时序。
  ASSERT_EQ(OB_SUCCESS, sql.assign("insert into test_table values (-1)"));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows));
  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_freeze_finish();
  share::SCN baseline_uploaded_ckpt_scn;
  ASSERT_EQ(OB_SUCCESS, get_ss_uploaded_minor_ckpt_scn(baseline_uploaded_ckpt_scn, 1200)); // 最多等 ~120s
  LOG_INFO("baseline ss tablet ready", K(baseline_uploaded_ckpt_scn), K(RunCtx.ls_id_), K(RunCtx.tablet_id_));

  // 关键：阻断后续常规增量上传的注册，制造未上传 mini 积压。
  // set_tp 需 SYS 租户权限，用 SYS 连接执行；tracepoint 为进程级，对 tt1 同样生效。
  SYS_EXE_SQL("alter system set_tp tp_name = LS_SS_INC_UPLOAD_SKIP_REGISTER, error_code = 4016, frequency = 1;");

  const int64_t kFreezeTimes = 5;
  for (int64_t i = 0; i < kFreezeTimes; ++i) {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into test_table values (%ld)", i));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows));
    EXE_SQL("alter system minor freeze tenant tt1;");
    wait_minor_freeze_finish();
  }

  // 给 compaction 调度多轮机会去尝试 local minor。
  sleep(10);

  // 校验场景状态：mini sstable 的上传被 errsim 阻断后，本地可能已经被普通 local minor 或 emergency
  // replace upload 调度压成较少的 minor，真实远端上传水位也可能追上本地 minor，不能把二者的
  // 相对大小当成稳定判据。
  int64_t minor_cnt = 0;
  share::SCN local_min_end_scn;
  share::SCN local_max_end_scn;
  share::SCN ss_uploaded_minor_ckpt_scn;
  ASSERT_EQ(OB_SUCCESS, get_local_minor_table_info(minor_cnt, local_min_end_scn, local_max_end_scn));
  ASSERT_EQ(OB_SUCCESS, get_ss_uploaded_minor_ckpt_scn(ss_uploaded_minor_ckpt_scn));
  LOG_INFO("normal state local minor count", K(minor_cnt), K(kFreezeTimes),
      K(local_min_end_scn), K(local_max_end_scn), K(ss_uploaded_minor_ckpt_scn));
  ASSERT_GT(minor_cnt, 0);
  ASSERT_TRUE(local_min_end_scn.is_valid_and_not_min());

  // 收尾：关闭 tracepoint（同样需 SYS 连接）
  SYS_EXE_SQL("alter system set_tp tp_name = LS_SS_INC_UPLOAD_SKIP_REGISTER, error_code = 4016, frequency = 0;");
}

// LOB-CTRL-MINOR-T001: emergency local Minor must still exclude unpublished Ctrl SSTables.
TEST_F(ObSSLocalMinorWatermarkTest, lob_ctrl_emergency_minor_checks_uploaded_checkpoint)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(RunCtx.tenant_id_));
  ASSERT_TRUE(GCTX.is_shared_storage_mode());
  ObSqlString sql;
  int64_t affected_rows = 0;
  // This case also works without running normal_state_un_uploaded_mini_not_merged first.
  EXE_SQL("create table if not exists test_table (a int)");
  set_ls_and_tablet_id_for_run_ctx();
  EXE_SQL("insert into test_table values (-2)");
  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_freeze_finish();
  share::SCN checkpoint;
  for (int64_t i = 0; i < 1200; ++i) {
    const int ret = get_ss_uploaded_minor_ckpt_scn(checkpoint, 1);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_TABLET_NOT_EXIST == ret);
    if (OB_SUCCESS == ret && checkpoint > share::SCN::base_scn()) {
      break;
    }
    usleep(100 * 1000);
  }
  ASSERT_TRUE(checkpoint > share::SCN::base_scn());

  ObLSHandle ls_handle;
  ObTabletHandle source;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(RunCtx.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS &ls = *ls_handle.get_ls();
  ASSERT_EQ(OB_SUCCESS, ls.get_tablet(RunCtx.tablet_id_, source,
      ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S, ObMDSGetTabletMode::READ_WITHOUT_CHECK));
  ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "CtrlMinorTest"));
  ObStorageSchema *source_schema = nullptr;
  DEFER(ObTabletObjLoadHelper::free(allocator, source_schema));
  ASSERT_EQ(OB_SUCCESS, source.get_obj()->load_storage_schema(allocator, source_schema));
  ObCreateTabletSchema create_schema;
  create_schema.table_id_ = RunCtx.tablet_id_.id();
  create_schema.data_table_id_ = create_schema.table_id_;
  create_schema.index_status_ = share::schema::INDEX_STATUS_AVAILABLE;
  ASSERT_EQ(OB_SUCCESS, create_schema.ObStorageSchema::init(allocator, *source_schema));

  // Build an isolated Tablet with a real StorageSchema and SSTable metadata. Never install it
  // into the LS: background uploads/compactions must not change these deterministic inputs.
  ObSEArray<ObTableHandleV2, 64> sstable_handles;
  ObTabletHandle candidate;
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::acquire_tmp_tablet(
      ObTabletMapKey(RunCtx.ls_id_, RunCtx.tablet_id_), allocator, candidate));
  ASSERT_EQ(OB_SUCCESS, candidate.get_obj()->init_for_first_time_creation(
      allocator, RunCtx.ls_id_, RunCtx.tablet_id_, RunCtx.tablet_id_,
      source.get_obj()->get_tablet_meta().create_scn_, source.get_obj()->get_snapshot_version(),
      create_schema, false /*need_empty_major*/, share::SCN::invalid_scn(),
      share::ObSplitTabletInfo(), share::ObForkTabletInfo(), false /*micro_index_clustered*/,
      false /*need_cs_cg*/, false /*has_cs_replica*/, DATA_CURRENT_VERSION, ls.get_freezer()));
  ObTablet &tablet = *candidate.get_obj();
  tablet.tablet_meta_.transfer_info_ = source.get_obj()->get_tablet_meta().transfer_info_;
  ASSERT_FALSE(tablet.get_tablet_meta().has_transfer_table());
  ASSERT_EQ(source.get_obj()->get_reorganization_scn(), tablet.get_reorganization_scn());
  ObStorageSchema &schema = *tablet.storage_schema_addr_.get_ptr();
  ObTabletTableStore &store = *tablet.table_store_addr_.get_ptr();
  ObSEArray<ObITable *, 64> tables;
  const int64_t count = ObPartitionMergePolicy::OB_EMERGENCY_TABLE_CNT;
  const int64_t uploaded_count = 3; // More than minor_compact_trigger (2), including end_scn == C.
  const int64_t checkpoint_value = checkpoint.get_val_for_tx();
  ASSERT_GT(checkpoint_value, uploaded_count);
  // The first unpublished file spans well beyond the current checkpoint. The remaining
  // files stay in the future, so unrelated background publication cannot make them uploaded.
  int64_t start = checkpoint_value - uploaded_count;
  for (int64_t i = 0; i < count; ++i) {
    const int64_t end = i < uploaded_count ? start + 1 : checkpoint_value + 86400L * 1000000000L + i;
    ObTabletCreateSSTableParam param;
    ASSERT_EQ(OB_SUCCESS, param.init_for_empty_major_sstable(
        RunCtx.tablet_id_, schema, 1 /*snapshot*/, -1 /*cg_idx*/, false, false));
    param.table_key_.table_type_ = ObITable::MINI_SSTABLE;
    ASSERT_EQ(OB_SUCCESS, param.table_key_.scn_range_.start_scn_.convert_for_tx(start));
    ASSERT_EQ(OB_SUCCESS, param.table_key_.scn_range_.end_scn_.convert_for_tx(end));
    param.max_merged_trans_version_ = i + 1;
    param.filled_tx_scn_ = param.table_key_.scn_range_.end_scn_;
    param.rec_scn_ = param.table_key_.scn_range_.start_scn_;
    void *buffer = allocator.alloc(sizeof(blocksstable::ObSSTable));
    ASSERT_NE(nullptr, buffer);
    blocksstable::ObSSTable *sstable = new (buffer) blocksstable::ObSSTable();
    bool owned_by_handle = false;
    DEFER(if (!owned_by_handle) {
      sstable->~ObSSTable();
      allocator.free(sstable);
    });
    ObTableHandleV2 handle;
    ASSERT_EQ(OB_SUCCESS, sstable->init(param, &allocator));
    ASSERT_EQ(OB_SUCCESS, handle.set_sstable(sstable, &allocator));
    owned_by_handle = true;
    ASSERT_EQ(OB_SUCCESS, sstable_handles.push_back(handle));
    ASSERT_EQ(OB_SUCCESS, tables.push_back(sstable));
    start = end;
  }
  ASSERT_EQ(OB_SUCCESS, store.minor_tables_.init(allocator, tables));
  ASSERT_EQ(OB_SUCCESS, tablet.table_store_cache_.init(store, schema, 0 /*max_medium_scn*/));
  ASSERT_EQ(count, tablet.get_minor_table_count());

  ObGetMergeTablesParam param;
  param.merge_type_ = MINOR_MERGE;
  ObGetMergeTablesResult result;
  schema.table_type_ = share::schema::AUX_OUTTABLE_LOB_CONTROL;
  ASSERT_EQ(OB_SUCCESS, tablet.table_store_cache_.init(store, schema, 0 /*max_medium_scn*/));
  ASSERT_TRUE(schema.is_aux_outtable_lob_control_table());
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::find_minor_merge_tables(
      param, 0 /*min_snapshot*/, INT64_MAX /*max_snapshot*/, ls, tablet, result));
  ASSERT_EQ(uploaded_count, result.handle_.get_count());
  ASSERT_EQ(checkpoint, result.scn_range_.end_scn_);
  for (int64_t i = 0; i < result.handle_.get_count(); ++i) {
    ASSERT_LE(result.handle_.get_table(i)->get_end_scn(), checkpoint);
    ASSERT_EQ(tables.at(i)->get_key(), result.handle_.get_table(i)->get_key());
  }

  // Older tablets without a cached type must preserve the same Ctrl checkpoint restriction.
  tablet.table_store_cache_.table_type_ = share::schema::MAX_TABLE_TYPE;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::find_minor_merge_tables(
      param, 0 /*min_snapshot*/, INT64_MAX /*max_snapshot*/, ls, tablet, result));
  ASSERT_EQ(uploaded_count, result.handle_.get_count());
  ASSERT_EQ(checkpoint, result.scn_range_.end_scn_);
  ASSERT_EQ(share::schema::MAX_TABLE_TYPE, tablet.table_store_cache_.get_table_type());

  // Control group: identical inputs as a normal table must retain the emergency bypass.
  schema.table_type_ = source_schema->get_table_type();
  ASSERT_EQ(OB_SUCCESS, tablet.table_store_cache_.init(store, schema, 0 /*max_medium_scn*/));
  ASSERT_FALSE(schema.is_aux_outtable_lob_control_table());
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::find_minor_merge_tables(
      param, 0 /*min_snapshot*/, INT64_MAX /*max_snapshot*/, ls, tablet, result));
  ASSERT_EQ(count, result.handle_.get_count());
  ASSERT_GT(result.scn_range_.end_scn_, checkpoint);

  // CTRL-MINOR-TRANSFER-001: the source plus local count reaches the emergency
  // threshold, although the local count alone does not. Reuse the published fixture
  // Tablet as source metadata; this isolated policy fixture is never installed as a transfer.
  ObTabletHandle ss_source;
  ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->get_tablet(
      RunCtx.ls_id_, RunCtx.tablet_id_, source.get_obj()->get_reorganization_scn(), allocator, ss_source));
  ASSERT_TRUE(ss_source.is_valid());
  ASSERT_GT(ss_source.get_obj()->get_minor_table_count(), 0);
  result.reset_handle_and_range();
  tables.pop_back();
  store.minor_tables_.reset();
  ASSERT_EQ(OB_SUCCESS, store.minor_tables_.init(allocator, tables));
  ASSERT_EQ(OB_SUCCESS, tablet.table_store_cache_.init(store, schema, 0 /*max_medium_scn*/));
  const int64_t local_count = tablet.get_minor_table_count();
  ASSERT_EQ(count - 1, local_count);
  ASSERT_LT(local_count, count);
  ASSERT_GE(local_count + ss_source.get_obj()->get_minor_table_count(), count);
  ObTabletTransferInfo &transfer_info = tablet.tablet_meta_.transfer_info_;
  transfer_info.ls_id_ = RunCtx.ls_id_;
  transfer_info.src_reorganization_scn_ = source.get_obj()->get_reorganization_scn();
  transfer_info.has_transfer_table_ = true;
  ASSERT_TRUE(tablet.get_tablet_meta().has_transfer_table());

  schema.table_type_ = share::schema::AUX_OUTTABLE_LOB_CONTROL;
  ASSERT_EQ(OB_SUCCESS, tablet.table_store_cache_.init(store, schema, 0 /*max_medium_scn*/));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::find_minor_merge_tables(
      param, 0 /*min_snapshot*/, INT64_MAX /*max_snapshot*/, ls, tablet, result));
  ASSERT_EQ(uploaded_count, result.handle_.get_count());
  ASSERT_EQ(checkpoint, result.scn_range_.end_scn_);
  for (int64_t i = 0; i < result.handle_.get_count(); ++i) {
    ASSERT_LE(result.handle_.get_table(i)->get_end_scn(), checkpoint);
    ASSERT_EQ(tables.at(i)->get_key(), result.handle_.get_table(i)->get_key());
  }
  schema.table_type_ = source_schema->get_table_type();
  ASSERT_EQ(OB_SUCCESS, tablet.table_store_cache_.init(store, schema, 0 /*max_medium_scn*/));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::find_minor_merge_tables(
      param, 0 /*min_snapshot*/, INT64_MAX /*max_snapshot*/, ls, tablet, result));
  ASSERT_EQ(local_count, result.handle_.get_count());
  ASSERT_GT(result.scn_range_.end_scn_, checkpoint);

  // CTRL-MINOR-TRANSFER-002: missing source metadata must not turn a failed
  // checkpoint decision into permission to merge the unpublished local files.
  transfer_info.ls_id_ = share::ObLSID(INT64_MAX - 1);
  ObTabletHandle missing_source;
  ASSERT_EQ(OB_TABLET_NOT_EXIST, MTL(ObSSMetaService *)->get_tablet(
      transfer_info.ls_id_, RunCtx.tablet_id_, transfer_info.src_reorganization_scn_,
      allocator, missing_source));
  schema.table_type_ = share::schema::AUX_OUTTABLE_LOB_CONTROL;
  ASSERT_EQ(OB_SUCCESS, tablet.table_store_cache_.init(store, schema, 0 /*max_medium_scn*/));
  ASSERT_EQ(OB_NO_NEED_MERGE, ObPartitionMergePolicy::find_minor_merge_tables(
      param, 0 /*min_snapshot*/, INT64_MAX /*max_snapshot*/, ls, tablet, result));
  ASSERT_EQ(0, result.handle_.get_count());
}

// LOB-COL-M001..M010: actual Ctrl SQL writes -> Mini -> public column iterator.
// Keep this in the existing shared-storage fixture so startup, object I/O and cleanup are reused.
TEST_F(ObSSLocalMinorWatermarkTest, lob_columns_real_ctrl_history)
{
  using namespace blocksstable;
  using namespace share::schema;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObMySQLProxy &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // Prevent background publication/minor from changing the history under test.
  SYS_EXE_SQL("alter system set_tp tp_name = LS_SS_INC_UPLOAD_SKIP_REGISTER, error_code = 4016, frequency = 1;");
  DEFER({
    int64_t affected = 0;
    EXPECT_EQ(OB_SUCCESS, proxy.write(
        "alter system set_tp tp_name = LS_SS_INC_UPLOAD_SKIP_REGISTER, error_code = 4016, frequency = 0;", affected));
  });

  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard));
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(OB_SYS_TENANT_ID,
      ObString::make_string("oceanbase"),
      ObString::make_string(share::OB_ALL_OUTTABLE_LOB_CONTROL_TNAME), false, table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_TRUE(table_schema->is_aux_outtable_lob_control_table());
  ObArray<ObTabletID> tablet_ids;
  ObArray<share::ObLSID> ls_ids;
  ASSERT_EQ(OB_SUCCESS, table_schema->get_tablet_ids(tablet_ids));
  ASSERT_EQ(1, tablet_ids.count()); // The sys-tenant control table has one partition.
  ASSERT_EQ(OB_SUCCESS, share::ObTabletToLSTableOperator::batch_get_ls(
      proxy, OB_SYS_TENANT_ID, tablet_ids, ls_ids));
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_ids.at(0), ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS &ls = *ls_handle.get_ls();
  const ObTabletID tablet_id = tablet_ids.at(0);
  const MacroBlockId object_a(MacroBlockId::DEFAULT_FIRST_ID, 2026101801, 0, 0, 0);
  const MacroBlockId object_c(MacroBlockId::DEFAULT_FIRST_ID, 2026101802, 0, 0, 0);
  const MacroBlockId rolled_back(MacroBlockId::DEFAULT_FIRST_ID, 2026101803, 0, 0, 0);
  const uint64_t hash = 0xfedcba9876543210ULL; // Exercise values above INT64_MAX.
  ObOuttableLobControlRow value;
  value.key_.macro_id_ = object_a;
  value.key_.hash_ = hash;
  value.status_ = OUTTABLE_LOB_CONTROL_DOING;

  ObSEArray<ObColDesc, 2> columns;
  const ObColumnSchemaV2 *hash_column = table_schema->get_column_schema("hash");
  const ObColumnSchemaV2 *id_column = table_schema->get_column_schema("macro_id");
  ASSERT_NE(nullptr, hash_column);
  ASSERT_NE(nullptr, id_column);
  ObColDesc col;
  col.col_id_ = hash_column->get_column_id();
  col.col_type_ = hash_column->get_meta_type();
  ASSERT_EQ(OB_SUCCESS, columns.push_back(col));
  col.col_id_ = id_column->get_column_id();
  col.col_type_ = id_column->get_meta_type();
  ASSERT_EQ(OB_SUCCESS, columns.push_back(col)); // Deliberately request hash before macro_id.

  const auto load_tablet = [&](ObTabletHandle &handle) {
    return ls.get_tablet(tablet_id, handle,
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  };
  const auto freeze_and_wait = [&](const bool require_checkpoint = true) {
    ObTabletHandle before;
    ASSERT_EQ(OB_SUCCESS, load_tablet(before));
    const share::SCN checkpoint = before.get_obj()->get_clog_checkpoint_scn();
    ASSERT_EQ(OB_SUCCESS, ls.tablet_freeze(tablet_id, true,
        ObTimeUtility::current_time() + 60 * 1000 * 1000, false, ObFreezeSourceFlag::TEST_MODE));
    bool finished = false;
    for (int64_t retry = 0; retry < 1200 && !finished; ++retry) {
      ObTabletHandle current;
      ASSERT_EQ(OB_SUCCESS, load_tablet(current));
      ObSEArray<ObTableHandleV2, 8> memtables;
      ASSERT_EQ(OB_SUCCESS, current.get_obj()->get_all_memtables_from_memtable_mgr(memtables));
      bool has_frozen = false;
      for (int64_t i = 0; i < memtables.count(); ++i) {
        has_frozen |= memtables.at(i).get_table()->is_frozen_memtable();
      }
      finished = !has_frozen && (!require_checkpoint
          || current.get_obj()->get_clog_checkpoint_scn() > checkpoint);
      if (!finished) { ob_usleep(100 * 1000); }
    }
    ASSERT_TRUE(finished);
  };
  // File metadata counts include DELETE rows; check the complete count and the chosen object ID.
  const auto check_scan = [&](ObTablet &tablet, const MacroBlockId &target,
      const bool expect_present) {
    ObTableStoreIterator files;
    ASSERT_EQ(OB_SUCCESS, tablet.get_all_sstables(files, false));
    int64_t expected_rows = 0;
    ObITable *file = nullptr;
    int file_ret = OB_SUCCESS;
    while (OB_SUCCESS == (file_ret = files.get_next(file))) {
      ObSSTableMetaHandle meta;
      ASSERT_EQ(OB_SUCCESS, static_cast<ObSSTable *>(file)->get_meta(meta));
      expected_rows += meta.get_sstable_meta().get_row_count();
    }
    ASSERT_EQ(OB_ITER_END, file_ret);
    ObLobColumnIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(tablet, columns, false));
    const ObDatumRow *row = nullptr;
    int64_t returned = 0;
    bool found = false;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = iter.get_next_row(row))) {
      ASSERT_NE(nullptr, row);
      ASSERT_EQ(2, row->count_);
      ++returned;
      ASSERT_LE(returned, expected_rows);
      MacroBlockId actual;
      int64_t pos = 0;
      const ObStorageDatum &datum = row->storage_datums_[1];
      ASSERT_EQ(OB_SUCCESS, actual.deserialize(datum.ptr_, datum.len_, pos));
      ASSERT_EQ(datum.len_, pos);
      if (actual == target) {
        ASSERT_EQ(hash, row->storage_datums_[0].get_uint64());
        found = true;
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(nullptr, row);
    ASSERT_EQ(expected_rows, returned);
    ASSERT_EQ(expect_present, found);
    ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
  };

  // M001: Memtable writes must not appear in a previously selected SSTable collection.
  ObTabletHandle selected;
  ASSERT_EQ(OB_SUCCESS, load_tablet(selected));
  ObLobColumnIterator fixed_iter;
  ASSERT_EQ(OB_SUCCESS, fixed_iter.init(*selected.get_obj(), columns, false));
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(proxy, OB_SYS_TENANT_ID, value, affected_rows));
  ASSERT_EQ(1, affected_rows);
  check_scan(*selected.get_obj(), object_a, false);
  ASSERT_FALSE(HasFatalFailure());
  freeze_and_wait();
  ASSERT_FALSE(HasFatalFailure());
  // M002: the fixed iterator must not switch to the newly installed Tablet/files.
  const ObDatumRow *fixed_row = nullptr;
  int fixed_ret = OB_SUCCESS;
  while (OB_SUCCESS == (fixed_ret = fixed_iter.get_next_row(fixed_row))) {
    MacroBlockId actual;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, actual.deserialize(fixed_row->storage_datums_[1].ptr_,
        fixed_row->storage_datums_[1].len_, pos));
    ASSERT_NE(object_a, actual);
  }
  ASSERT_EQ(OB_ITER_END, fixed_ret);

  ObTabletHandle current;
  ASSERT_EQ(OB_SUCCESS, load_tablet(current));
  check_scan(*current.get_obj(), object_a, true);
  ASSERT_FALSE(HasFatalFailure());
  // M003: INSERT -> UPDATE -> DELETE, with a separate Mini for each stage.
  value.status_ = OUTTABLE_LOB_CONTROL_COMPLETE;
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::update(proxy, OB_SYS_TENANT_ID, value, affected_rows));
  freeze_and_wait();
  ASSERT_FALSE(HasFatalFailure());
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::delete_row(proxy, OB_SYS_TENANT_ID, object_a, hash, affected_rows));
  freeze_and_wait();
  ASSERT_FALSE(HasFatalFailure());
  current.reset();
  ASSERT_EQ(OB_SUCCESS, load_tablet(current));
  ASSERT_GE(current.get_obj()->get_minor_table_count(), 3);
  check_scan(*current.get_obj(), object_a, true);
  ASSERT_FALSE(HasFatalFailure());
  // M004: DELETE -> INSERT across files; the earlier DELETE must still be returned.
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(proxy, OB_SYS_TENANT_ID, value, affected_rows));
  freeze_and_wait();
  ASSERT_FALSE(HasFatalFailure());
  current.reset();
  ASSERT_EQ(OB_SUCCESS, load_tablet(current));
  check_scan(*current.get_obj(), object_a, true);
  ASSERT_FALSE(HasFatalFailure());
  // M005: DELETE -> INSERT in one Memtable. Read all rows Mini actually saved.
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::delete_row(proxy, OB_SYS_TENANT_ID, object_a, hash, affected_rows));
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(proxy, OB_SYS_TENANT_ID, value, affected_rows));
  freeze_and_wait();
  ASSERT_FALSE(HasFatalFailure());
  current.reset();
  ASSERT_EQ(OB_SUCCESS, load_tablet(current));
  check_scan(*current.get_obj(), object_a, true);
  ASSERT_FALSE(HasFatalFailure());
  // M006: INSERT -> DELETE in one Memtable. Do not invent versions Mini already folded.
  value.key_.macro_id_ = object_c;
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(proxy, OB_SYS_TENANT_ID, value, affected_rows));
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::delete_row(proxy, OB_SYS_TENANT_ID, object_c, hash, affected_rows));
  freeze_and_wait(false);
  ASSERT_FALSE(HasFatalFailure());
  current.reset();
  ASSERT_EQ(OB_SUCCESS, load_tablet(current));
  // Whether C survives Mini is outside this interface's contract. Validate complete physical count.
  check_scan(*current.get_obj(), object_a, true);
  ASSERT_FALSE(HasFatalFailure());
  // M007: verify SQL rollback, then verify physical reading independently of SQL visibility.
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&proxy, OB_SYS_TENANT_ID));
  DEFER(if (trans.is_started()) { EXPECT_EQ(OB_SUCCESS, trans.end(false)); });
  value.key_.macro_id_ = rolled_back;
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(trans, OB_SYS_TENANT_ID, value, affected_rows));
  ASSERT_EQ(OB_SUCCESS, trans.end(false));
  ObArenaAllocator rollback_allocator("LobGCRollback");
  ObOuttableLobControlRow selected_row;
  int64_t row_scn = 0;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObOuttableLobControlOperator::select(proxy, OB_SYS_TENANT_ID,
      rolled_back, hash, false, rollback_allocator, row_scn, selected_row));
  // Add one committed row to guarantee this Mini has data even if rollback removes its node.
  value.key_.macro_id_ = object_a;
  value.status_ = OUTTABLE_LOB_CONTROL_FAIL; // Force a real change, even if repeated SQL UPDATEs are optimized.
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::update(proxy, OB_SYS_TENANT_ID, value, affected_rows));
  freeze_and_wait();
  ASSERT_FALSE(HasFatalFailure());
  current.reset();
  ASSERT_EQ(OB_SUCCESS, load_tablet(current));
  check_scan(*current.get_obj(), object_a, true);
  ASSERT_FALSE(HasFatalFailure());
  // M009: enough incompressible binary data for a real multi-macro Mini SSTable.
  char binary[32768];
  uint64_t seed = 0x1122334455667788ULL;
  for (int64_t i = 0; i < 100; ++i) {
    for (int64_t j = 0; j < sizeof(binary); ++j) {
      seed ^= seed << 13;
      seed ^= seed >> 7;
      seed ^= seed << 17;
      binary[j] = static_cast<char>(seed);
    }
    value.key_.macro_id_ = MacroBlockId(MacroBlockId::DEFAULT_FIRST_ID, 2026101900 + i, 0, 0, 0);
    value.info_.assign_ptr(binary, sizeof(binary));
    ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(proxy, OB_SYS_TENANT_ID, value, affected_rows));
  }
  freeze_and_wait();
  ASSERT_FALSE(HasFatalFailure());
  current.reset();
  ASSERT_EQ(OB_SUCCESS, load_tablet(current));
  ObTableStoreIterator files;
  ASSERT_EQ(OB_SUCCESS, current.get_obj()->get_all_sstables(files, false));
  int64_t max_macro_count = 0;
  ObITable *file = nullptr;
  int file_ret = OB_SUCCESS;
  while (OB_SUCCESS == (file_ret = files.get_next(file))) {
    ObSSTableMetaHandle meta;
    ASSERT_EQ(OB_SUCCESS, static_cast<ObSSTable *>(file)->get_meta(meta));
    max_macro_count = MAX(max_macro_count, meta.get_sstable_meta().get_data_macro_block_count());
  }
  ASSERT_EQ(OB_ITER_END, file_ret);
  ASSERT_GE(max_macro_count, 2);
  check_scan(*current.get_obj(), value.key_.macro_id_, true);
  ASSERT_FALSE(HasFatalFailure());
  // M010: a decoder failure invalidates the scan; release it before starting a fresh scan.
  const ObDatumRow *partial = nullptr;
  {
    ObLobColumnIterator faulted;
    ASSERT_EQ(OB_SUCCESS, faulted.init(*current.get_obj(), columns, false));
    ASSERT_EQ(OB_SUCCESS, faulted.get_next_row(partial));
    ASSERT_NE(nullptr, partial);
    // Test-only corruption of the reader cursor: force get_row() to reject an out-of-range row.
    ASSERT_NE(nullptr, faulted.macro_iter_);
    faulted.macro_iter_->curr_block_row_cnt_ = INT64_MAX;
    faulted.macro_iter_->curr_block_row_idx_ = INT64_MAX - 1;
    const int decode_ret = faulted.get_next_row(partial);
    ASSERT_NE(OB_SUCCESS, decode_ret);
    ASSERT_NE(OB_ITER_END, decode_ret);
    ASSERT_EQ(nullptr, partial);
  }
  // A fresh scan must still succeed after the failed iterator was released.
  check_scan(*current.get_obj(), value.key_.macro_id_, true);
  ASSERT_FALSE(HasFatalFailure());
  // M008: nonexistent columns must fail during initialization, including on real file-backed Tablets.
  ObSEArray<ObColDesc, 2> missing;
  ASSERT_EQ(OB_SUCCESS, missing.assign(columns));
  missing.at(0).col_id_ += 10000;
  {
    ObLobColumnIterator bad;
    ASSERT_EQ(OB_INVALID_ARGUMENT, bad.init(*current.get_obj(), missing, false));
  }

  // LOB-COL-MINOR-REPLACE-001: a partially consumed iterator must survive real local Minor
  // installation. Compare physical versions, including the two MVCC keys, before/after replacement.
  files.reset(); // Do not let the earlier metadata enumeration keep extra SSTable handles alive.
  ObTabletHandle before_minor;
  ASSERT_EQ(OB_SUCCESS, load_tablet(before_minor));
  ObSEArray<ObITable::TableKey, 16> old_minor_keys;
  int64_t old_row_count = 0;
  {
    ObTableStoreIterator old_files;
    ASSERT_EQ(OB_SUCCESS, before_minor.get_obj()->get_all_sstables(old_files, false));
    while (OB_SUCCESS == (file_ret = old_files.get_next(file))) {
      ObSSTableMetaHandle meta;
      ASSERT_EQ(OB_SUCCESS, static_cast<ObSSTable *>(file)->get_meta(meta));
      old_row_count += meta.get_sstable_meta().get_row_count();
      if (file->is_minor_sstable()) {
        ASSERT_EQ(OB_SUCCESS, old_minor_keys.push_back(file->get_key()));
      }
    }
    ASSERT_EQ(OB_ITER_END, file_ret);
  }
  ASSERT_GE(old_minor_keys.count(), 3);
  ObSEArray<ObColDesc, 4> version_columns;
  ASSERT_EQ(OB_SUCCESS, version_columns.assign(columns));
  for (const uint64_t id : {OB_HIDDEN_TRANS_VERSION_COLUMN_ID, OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID}) {
    ObColDesc hidden;
    hidden.col_id_ = id;
    hidden.col_type_.set_int();
    ASSERT_EQ(OB_SUCCESS, version_columns.push_back(hidden));
  }
  struct SavedVersion
  {
    MacroBlockId object_id_;
    uint64_t hash_;
    int64_t trans_version_;
    int64_t sql_sequence_;
    TO_STRING_KV(K_(object_id), K_(hash), K_(trans_version), K_(sql_sequence));
  };
  const auto save_version = [](const ObDatumRow &row, SavedVersion &saved) {
    int64_t pos = 0;
    const ObStorageDatum &id = row.storage_datums_[1];
    const int ret = saved.object_id_.deserialize(id.ptr_, id.len_, pos);
    if (OB_SUCCESS == ret && pos == id.len_) {
      saved.hash_ = row.storage_datums_[0].get_uint64();
      saved.trans_version_ = row.storage_datums_[2].get_int();
      saved.sql_sequence_ = row.storage_datums_[3].get_int();
    }
    return OB_SUCCESS != ret ? ret : (pos == id.len_ ? OB_SUCCESS : OB_INVALID_DATA);
  };
  ObSEArray<SavedVersion, 128> expected_versions;
  {
    ObLobColumnIterator baseline;
    ASSERT_EQ(OB_SUCCESS, baseline.init(*before_minor.get_obj(), version_columns, false));
    while (OB_SUCCESS == (file_ret = baseline.get_next_row(partial))) {
      ASSERT_NE(nullptr, partial);
      ASSERT_EQ(4, partial->count_);
      SavedVersion saved;
      ASSERT_EQ(OB_SUCCESS, save_version(*partial, saved));
      ASSERT_EQ(OB_SUCCESS, expected_versions.push_back(saved));
    }
    ASSERT_EQ(OB_ITER_END, file_ret);
  }
  ASSERT_EQ(old_row_count, expected_versions.count());
  ASSERT_GT(expected_versions.count(), 1);
  ObLobColumnIterator paused;
  ASSERT_EQ(OB_SUCCESS, paused.init(*before_minor.get_obj(), version_columns, false));
  ASSERT_EQ(OB_SUCCESS, paused.get_next_row(partial));
  ASSERT_NE(nullptr, partial);
  ASSERT_EQ(4, partial->count_);
  SavedVersion first_version;
  ASSERT_EQ(OB_SUCCESS, save_version(*partial, first_version));

  // Block shared Minor execution for this Tablet, so a shared-file link cannot satisfy the check.
  ObSqlString block_shared_minor;
  ASSERT_EQ(OB_SUCCESS, block_shared_minor.assign_fmt(
      "alter system set_tp tp_name = EN_SS_MINOR_MERGE, error_code = 4023, frequency = 1, match = %ld;",
      (ls.get_ls_id().id() << 37) | static_cast<int64_t>(tablet_id.id())));
  ASSERT_EQ(OB_SUCCESS, proxy.write(block_shared_minor.ptr(), affected_rows));
  DEFER({
    int64_t affected = 0;
    EXPECT_EQ(OB_SUCCESS, proxy.write(
        "alter system set_tp tp_name = EN_SS_MINOR_MERGE, error_code = 4023, frequency = 0;", affected));
  });
  SYS_EXE_SQL("alter system set_tp tp_name = LS_SS_INC_UPLOAD_SKIP_REGISTER, error_code = 4016, frequency = 0;");
  share::SCN uploaded = share::SCN::base_scn();
  const share::SCN old_end = old_minor_keys.at(old_minor_keys.count() - 1).scn_range_.end_scn_;
  for (int64_t retry = 0; retry < 1200 && uploaded < old_end; ++retry) {
    ObArenaAllocator allocator("LobIterUpload");
    ObTabletHandle shared;
    const int ret = MTL(ObSSMetaService *)->get_tablet(ls.get_ls_id(), tablet_id,
        before_minor.get_obj()->get_reorganization_scn(), allocator, shared);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_TABLET_NOT_EXIST == ret) << ret;
    if (OB_SUCCESS == ret && shared.is_valid()) {
      uploaded = shared.get_obj()->get_clog_checkpoint_scn();
    }
    if (uploaded < old_end) { ob_usleep(100 * 1000); }
  }
  ASSERT_GE(uploaded, old_end);

  ObTabletHandle after_minor;
  bool replaced = false;
  for (int64_t retry = 0; retry < 1200 && !replaced; ++retry) {
    after_minor.reset();
    ASSERT_EQ(OB_SUCCESS, load_tablet(after_minor));
    ObSEArray<ObITable::TableKey, 16> current_keys;
    ObITable::TableKey replacement_key;
    {
      ObTableStoreIterator current_files;
      ASSERT_EQ(OB_SUCCESS, after_minor.get_obj()->get_all_sstables(current_files, false));
      while (OB_SUCCESS == (file_ret = current_files.get_next(file))) {
        ASSERT_EQ(OB_SUCCESS, current_keys.push_back(file->get_key()));
        if (ObITable::MINOR_SSTABLE == file->get_key().table_type_) {
          ObSSTableMetaHandle meta;
          ASSERT_EQ(OB_SUCCESS, static_cast<ObSSTable *>(file)->get_meta(meta));
          int64_t covered_count = 0;
          for (int64_t i = 0; i < old_minor_keys.count(); ++i) {
            const ObITable::TableKey &old_key = old_minor_keys.at(i);
            covered_count += file->get_start_scn() <= old_key.scn_range_.start_scn_
                && file->get_end_scn() >= old_key.scn_range_.end_scn_ ? 1 : 0;
          }
          if (!meta.get_sstable_meta().is_shared_table() && covered_count >= 2) {
            replacement_key = file->get_key();
            replaced = true;
          }
        }
      }
      ASSERT_EQ(OB_ITER_END, file_ret);
    }
    if (replaced) {
      ASSERT_NE(before_minor.get_obj(), after_minor.get_obj());
      for (int64_t i = 0; i < old_minor_keys.count(); ++i) {
        const ObITable::TableKey &old_key = old_minor_keys.at(i);
        if (replacement_key.scn_range_.start_scn_ <= old_key.scn_range_.start_scn_
            && replacement_key.scn_range_.end_scn_ >= old_key.scn_range_.end_scn_) {
          for (int64_t j = 0; j < current_keys.count(); ++j) {
            ASSERT_NE(old_key, current_keys.at(j));
          }
        }
      }
    } else {
      ObGetMergeTablesParam minor_param;
      minor_param.merge_type_ = MINOR_MERGE;
      ObGetMergeTablesResult merge_result;
      const int select_ret = ObPartitionMergePolicy::find_minor_merge_tables(
          minor_param, 0, INT64_MAX, ls, *after_minor.get_obj(), merge_result);
      ASSERT_TRUE(OB_SUCCESS == select_ret || OB_NO_NEED_MERGE == select_ret) << select_ret;
      if (OB_SUCCESS == select_ret) {
        ASSERT_GE(merge_result.handle_.get_count(), 2);
        ASSERT_LE(merge_result.scn_range_.end_scn_, uploaded);
        ObTabletMergeDagParam dag_param(MINOR_MERGE, ls.get_ls_id(), tablet_id,
            merge_result.private_transfer_epoch_);
        ASSERT_EQ(OB_SUCCESS, ObTenantTabletScheduler::fill_minor_compaction_param(
            *after_minor.get_obj(), merge_result, merge_result.handle_.get_count(), 1,
            ObTimeUtility::current_time(), dag_param));
        const int schedule_ret = ObTenantTabletScheduler::schedule_merge_execute_dag<ObTabletMergeExecuteDag>(
            dag_param, ls_handle, after_minor, merge_result);
        ASSERT_TRUE(OB_SUCCESS == schedule_ret || OB_EAGAIN == schedule_ret) << schedule_ret;
      }
      ob_usleep(100 * 1000);
    }
  }
  ASSERT_TRUE(replaced);
  // Resume only after old inputs have left the current TableStore. Old handles remain alive.
  for (int64_t i = 0; i < expected_versions.count(); ++i) {
    SavedVersion actual;
    if (0 == i) {
      actual = first_version;
    } else {
      ASSERT_EQ(OB_SUCCESS, paused.get_next_row(partial));
      ASSERT_NE(nullptr, partial);
      ASSERT_EQ(4, partial->count_);
      ASSERT_EQ(OB_SUCCESS, save_version(*partial, actual));
    }
    const SavedVersion &expected = expected_versions.at(i);
    ASSERT_EQ(expected.object_id_, actual.object_id_);
    ASSERT_EQ(expected.hash_, actual.hash_);
    ASSERT_EQ(expected.trans_version_, actual.trans_version_);
    ASSERT_EQ(expected.sql_sequence_, actual.sql_sequence_);
  }
  ASSERT_EQ(OB_ITER_END, paused.get_next_row(partial));
  ASSERT_EQ(nullptr, partial);
  // A new iterator reads the installed Minor; its count is checked against the new file metadata.
  check_scan(*after_minor.get_obj(), value.key_.macro_id_, true);
  ASSERT_FALSE(HasFatalFailure());
}

static void verify_lob_physical_backup_restore(
    const ObTablet &tablet, const blocksstable::MacroBlockId &object_id,
    const ObLSID &ls_id, const ObString &source)
{
  using namespace blocksstable;
  using namespace share;
  const uint64_t tenant_id = MTL_ID();
  ObObjectStorageTenantGuard storage_guard(tenant_id, 60 * 1000 * 1000L);
  ObArenaAllocator allocator(ObMemAttr(tenant_id, "LobBackupTest"));
  const int64_t object_size = source.length();
  DEFER({
    const int cleanup_ret = ObObjectManager::delete_object(object_id, 0 /*ls_epoch*/);
    EXPECT_TRUE(OB_SUCCESS == cleanup_ret || OB_OBJECT_NOT_EXIST == cleanup_ret);
  });
  // Collection reads the stored length from Meta before the object exists.
  ObSEArray<ObTabletBlockInfo, 1> blocks;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaTabletMacroIDIterator::collect_lob_blocks_(tablet, blocks));
  ASSERT_EQ(1, blocks.count()); // Only the OUTTABLE data row supplies an object reference.
  for (int64_t i = 0; i < blocks.count(); ++i) {
    ASSERT_EQ(object_id, blocks.at(i).macro_id());
    ASSERT_EQ(object_size, blocks.at(i).occupy_size());
  }
  // The task generator deduplicates candidates; copy this object once.
  ObSSHAMacroBlockInfo macro_info;
  macro_info.ls_id_ = ls_id;
  macro_info.macro_block_id_ = blocks.at(0).macro_id();
  macro_info.occupy_size_ = blocks.at(0).occupy_size();
  ObSSHAMacroBlockBatch macro_batch;
  ASSERT_EQ(OB_SUCCESS, macro_batch.add_one(macro_info));

  ObBackupDest backup_dest;
  ASSERT_EQ(OB_SUCCESS, backup_dest.set(shared_storage_info));
  ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = 2026102914;
  backup_set_desc.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  ObBackupDest backup_set_dest;
  ASSERT_EQ(OB_SUCCESS, ObBackupPathUtil::construct_backup_set_dest(
      backup_dest, backup_set_desc, backup_set_dest));
  const int64_t dest_id = 1;
  const ObStorageIdMod backup_mod(dest_id, ObStorageUsedMod::STORAGE_USED_BACKUP);
  ObBackupIOInfo io_info;
  io_info.max_iops_ = backup_set_dest.get_storage_info()->max_iops_;
  io_info.max_bandwidth_ = backup_set_dest.get_storage_info()->max_bandwidth_;
  ASSERT_EQ(OB_SUCCESS, ObBackupStorageInfoOperator::insert_backup_storage_info(
      *GCTX.sql_proxy_, tenant_id, backup_set_dest, ObBackupDestType::DEST_TYPE_RESTORE_DATA,
      dest_id, io_info, false /*can_update*/));
  DEFER({
    EXPECT_EQ(OB_SUCCESS, ObBackupStorageInfoOperator::remove_backup_storage_info(
        *GCTX.sql_proxy_, tenant_id, backup_set_dest));
  });
  ObDagId dag_id;
  dag_id.init(GCTX.self_addr());
  ObSysTaskStat task;
  task.start_time_ = ObTimeUtility::current_time();
  task.task_id_ = dag_id;
  task.task_type_ = SS_HA_MACRO_COPY_TASK;
  task.tenant_id_ = tenant_id;
  ASSERT_EQ(OB_SUCCESS, SYS_TASK_STATUS_MGR.add_task(task));
  DEFER({ EXPECT_EQ(OB_SUCCESS, SYS_TASK_STATUS_MGR.del_task(dag_id)); });
  ObBackupPath backup_set_dir;
  ObBackupPath backup_path;
  ASSERT_EQ(OB_SUCCESS, ObBackupPathUtil::get_backup_set_dir_path(backup_set_dest, backup_set_dir));
  ASSERT_EQ(OB_SUCCESS, backup::ObSSBackupUtils::get_macro_block_backup_path(
      object_id, backup_set_dir, backup_path));
  DEFER({
    const int cleanup_ret = ObBackupIoAdapter::del_file(
        backup_path.get_obstr(), backup_set_dest.get_storage_info());
    EXPECT_TRUE(OB_SUCCESS == cleanup_ret || OB_OBJECT_NOT_EXIST == cleanup_ret);
  });

  // The scheduling stage handles the wait; copying a missing LOB returns the error directly.
  ObCopySSMacroBlockBackupReader missing_reader;
  ASSERT_EQ(OB_SUCCESS, missing_reader.init(backup_dest, backup_set_desc, backup_mod, macro_batch));
  ObBufferReader missing_data;
  MacroBlockId missing_id;
  ASSERT_EQ(OB_OBJECT_NOT_EXIST, missing_reader.get_next_macro_block(allocator, missing_data, missing_id));
  // This direct object write generates no LOB redo; the Meta SSTable is isolated from the live LS.
  ASSERT_EQ(OB_SUCCESS, ObLobOutTableIO::write(object_id, source));
  ObCopySSMacroBlockBackupReader backup_reader;
  ASSERT_EQ(OB_SUCCESS, backup_reader.init(backup_dest, backup_set_desc, backup_mod, macro_batch));
  ObCopySSMacroBlockBackupWriter backup_writer;
  ASSERT_EQ(OB_SUCCESS, backup_writer.init(
      tenant_id, dag_id, backup_dest, backup_set_desc, backup_mod, &backup_reader));
  ASSERT_EQ(OB_SUCCESS, backup_writer.process());
  ASSERT_EQ(object_size, backup_reader.get_data_size());
  int64_t backup_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObBackupIoAdapter::get_file_length(
      backup_path.get_obstr(), backup_set_dest.get_storage_info(), backup_size));
  ASSERT_EQ(object_size, backup_size);
  // LOB backup files contain the source bytes directly, without an extra header.
  char *backup_bytes = static_cast<char *>(allocator.alloc(object_size));
  ASSERT_NE(nullptr, backup_bytes);
  int64_t backup_read_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObBackupIoAdapter::read_single_file(
      backup_path.get_obstr(), backup_set_dest.get_storage_info(),
      backup_bytes, object_size, backup_read_size, backup_mod));
  ASSERT_EQ(object_size, backup_read_size);
  ASSERT_EQ(0, MEMCMP(source.ptr(), backup_bytes, object_size));

  // Remove both the source object and its local cache before restoring from the backup file.
  ASSERT_EQ(OB_SUCCESS, ObObjectManager::delete_object(object_id, 0 /*ls_epoch*/));
  bool exists = true;
  ASSERT_EQ(OB_SUCCESS, ObRemoteIoAdapter::is_exist(tenant_id, object_id, GCONF.observer_id, exists));
  ASSERT_FALSE(exists);
  ObCopySSMacroBlockRestoreReader restore_reader;
  ASSERT_EQ(OB_SUCCESS, restore_reader.init(tenant_id, backup_set_dest, macro_batch));
  ObCopySSMacroBlockRestoreWriter restore_writer;
  ASSERT_EQ(OB_SUCCESS, restore_writer.init(tenant_id, ls_id, dag_id, &restore_reader));
  // A full 2 MiB LOB object fits the default restore buffer without an extra header.
  ASSERT_EQ(OB_SUCCESS, restore_writer.process());
  ASSERT_EQ(object_size, restore_reader.get_data_size());
  ASSERT_EQ(OB_SUCCESS, ObRemoteIoAdapter::is_exist(tenant_id, object_id, GCONF.observer_id, exists));
  ASSERT_TRUE(exists);

  // Read the restored remote bytes directly so an old cache entry cannot satisfy the assertion.
  ObBaseFileManager *file_manager = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::get_file_manager(tenant_id, file_manager));
  ASSERT_NE(nullptr, file_manager);
  ObBackupDest storage_dest;
  ASSERT_EQ(OB_SUCCESS, file_manager->get_storage_dest(storage_dest));
  ObPathContext object_path;
  ASSERT_EQ(OB_SUCCESS, object_path.set_file_ctx(object_id, 0, false /*is_local_cache*/, GCONF.observer_id));
  // Check the complete remote file length, including any unexpected trailing padding.
  int64_t restored_file_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObBackupIoAdapter::get_file_length(
      ObString::make_string(object_path.get_path()), storage_dest.get_storage_info(), restored_file_size));
  ASSERT_EQ(object_size, restored_file_size);
  char *restored = static_cast<char *>(allocator.alloc(object_size));
  ASSERT_NE(nullptr, restored);
  int64_t restored_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObBackupIoAdapter::read_single_file(
      ObString::make_string(object_path.get_path()), storage_dest.get_storage_info(),
      restored, object_size, restored_size, ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(object_size, restored_size);
  ASSERT_EQ(0, MEMCMP(source.ptr(), restored, object_size));
}

TEST_F(ObSSLocalMinorWatermarkTest, lob_ctrl_does_not_collect_objects)
{
  using namespace blocksstable;
  using namespace share::schema;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObSchemaGetterGuard guard;
  const ObTableSchema *control_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard));
  ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(OB_SYS_TENANT_ID,
      ObString::make_string("oceanbase"), ObString::make_string(share::OB_ALL_OUTTABLE_LOB_CONTROL_TNAME),
      false, control_schema));
  ASSERT_NE(nullptr, control_schema);
  ObArray<ObTabletID> tablet_ids;
  ObArray<share::ObLSID> ls_ids;
  ASSERT_EQ(OB_SUCCESS, control_schema->get_tablet_ids(tablet_ids));
  ASSERT_EQ(1, tablet_ids.count());
  ASSERT_EQ(OB_SUCCESS, share::ObTabletToLSTableOperator::batch_get_ls(
      get_curr_simple_server().get_observer().get_mysql_proxy(), OB_SYS_TENANT_ID, tablet_ids, ls_ids));
  const ObTabletID tablet_id = tablet_ids.at(0);
  MacroBlockId object_id;
  object_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
  object_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
  object_id.set_second_id(2026103001);
  object_id.set_third_id(tablet_id.id());
  object_id.set_fourth_id(INT64_MAX - 1);
  object_id.set_fifth_id(0);
  ASSERT_TRUE(object_id.is_valid());

  ObArenaAllocator allocator("LobCtrlBackup");
  ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, table_schema.assign(*control_schema));
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, table_schema.set_compress_func_name("none"));
  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(2, storage_schema.get_rowkey_column_num());
  ASSERT_EQ(5, storage_schema.get_column_count());
  ObWholeDataStoreDesc data_desc;
  share::SCN end_scn;
  ASSERT_EQ(OB_SUCCESS, end_scn.convert_for_tx(200));
  ASSERT_EQ(OB_SUCCESS, data_desc.init(false, table_schema, ls_ids.at(0), tablet_id,
      MINOR_MERGE, 200, DATA_CURRENT_VERSION, false, 0, 0, share::SCN::min_scn(), end_scn,
      nullptr, 0, EXEC_MODE_LOCAL));
  ObSSTableIndexBuilder builder(false);
  data_desc.get_desc().sstable_index_builder_ = &builder;
  ObWholeDataStoreDesc index_desc;
  ASSERT_EQ(OB_SUCCESS, index_desc.gen_index_store_desc(data_desc.get_desc()));
  ASSERT_EQ(OB_SUCCESS, builder.init(index_desc.get_desc()));
  ObSSTablePrivateObjectCleaner cleaner;
  ObMacroBlockWriter writer;
  ObMacroDataSeq sequence(0);
  sequence.set_data_block();
  ObMacroSeqParam sequence_param;
  sequence_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  sequence_param.start_ = sequence.macro_data_seq_ + 2000;
  ObPreWarmerParam prewarm(MEM_PRE_WARM);
  ASSERT_EQ(OB_SUCCESS, writer.open(data_desc.get_desc(), 0, sequence_param, prewarm, cleaner));
  ObDatumRow input;
  ASSERT_EQ(OB_SUCCESS, input.init(allocator, 7));
  char object_buf[128];
  int64_t object_len = 0;
  ASSERT_EQ(OB_SUCCESS, object_id.serialize(object_buf, sizeof(object_buf), object_len));
  const int64_t statuses[] = {OUTTABLE_LOB_CONTROL_DOING, OUTTABLE_LOB_CONTROL_FAIL,
                            OUTTABLE_LOB_CONTROL_COMPLETE};
  for (int64_t i = 0; i < 4; ++i) {
    input.row_flag_.set_flag(i == 3 ? DF_DELETE : DF_INSERT);
    input.mvcc_row_flag_.reset();
    input.mvcc_row_flag_.set_first_multi_version_row(true);
    input.mvcc_row_flag_.set_last_multi_version_row(true);
    input.mvcc_row_flag_.set_compacted_multi_version_row(true);
    input.storage_datums_[0].set_string(object_buf, object_len); // macro_id
    input.storage_datums_[1].set_uint(i); // hash
    input.storage_datums_[2].set_int(-100); // hidden transaction version
    input.storage_datums_[3].set_int(0); // hidden SQL sequence
    if (i == 3) {
      for (int64_t j = 4; j < 7; ++j) { input.storage_datums_[j].set_nop(); }
    } else {
      input.storage_datums_[4].set_int(statuses[i]);
      input.storage_datums_[5].set_int(100);
      input.storage_datums_[6].set_null(); // info is unrelated to object collection.
    }
    ASSERT_EQ(OB_SUCCESS, writer.append_row(input));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ObSSTableMergeRes res;
  ASSERT_EQ(OB_SUCCESS, builder.close(res));
  ObTabletCreateSSTableParam param;
  ASSERT_EQ(OB_SUCCESS, param.init_for_empty_major_sstable(tablet_id, storage_schema, 200, -1, false, false));
  ASSERT_EQ(OB_SUCCESS, param.inner_init_with_merge_res(res));
  param.nested_size_ = res.nested_size_;
  param.nested_offset_ = res.nested_offset_;
  param.max_merged_trans_version_ = res.max_merged_trans_version_;
  param.min_merged_trans_version_ = res.min_merged_trans_version_;
  param.table_key_.table_type_ = ObITable::MINOR_SSTABLE;
  ASSERT_EQ(OB_SUCCESS, param.table_key_.scn_range_.start_scn_.convert_for_tx(1));
  param.table_key_.scn_range_.end_scn_ = end_scn;
  param.filled_tx_scn_ = end_scn;
  param.rec_scn_ = param.table_key_.scn_range_.start_scn_;
  ObTableHandleV2 file_handle;
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable(param, allocator, file_handle));
  ObTabletTableStore store;
  ObTablet isolated;
  DEFER({ isolated.storage_schema_addr_.ptr_ = nullptr; isolated.table_store_addr_.ptr_ = nullptr; });
  store.is_inited_ = true;
  isolated.is_inited_ = true;
  isolated.storage_schema_addr_.ptr_ = &storage_schema;
  isolated.table_store_addr_.ptr_ = &store;
  ObSEArray<ObITable *, 1> one_file;
  ASSERT_EQ(OB_SUCCESS, one_file.push_back(file_handle.get_table()));
  ASSERT_EQ(OB_SUCCESS, store.minor_tables_.init(allocator, one_file));
  // Even an existing object recorded in Ctrl is not an additional backup candidate.
  ObObjectStorageTenantGuard storage_guard(MTL_ID(), 60 * 1000 * 1000L);
  DEFER({ EXPECT_EQ(OB_SUCCESS, ObObjectManager::delete_object(object_id, 0 /*ls_epoch*/)); });
  ASSERT_EQ(OB_SUCCESS, ObLobOutTableIO::write(object_id, ObString::make_string("ctrl_only")));
  ObSEArray<ObTabletBlockInfo, 1> blocks;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaTabletMacroIDIterator::collect_lob_blocks_(isolated, blocks));
  ASSERT_TRUE(blocks.empty());
}

void ObSSLocalMinorWatermarkTest::create_lob_meta_sstable(
    common::ObArenaAllocator &allocator,
    const share::schema::ObTableSchema &meta_table_schema,
    const ObStorageSchema &meta_schema,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObMergeType merge,
    const int64_t object_size,
    const int64_t file_idx,
    const blocksstable::MacroBlockId &reference,
    blocksstable::ObSSTablePrivateObjectCleaner &cleaner,
    ObTableHandleV2 &handle)
{
  using namespace blocksstable;
  const auto layout = meta_table_schema.get_row_store_type();
  const bool is_minor = MINOR_MERGE == merge;
  const int64_t snapshot = 200 * (file_idx + 1);
  ObWholeDataStoreDesc data_desc;
  share::SCN start_scn;
  share::SCN end_scn;
  ASSERT_EQ(OB_SUCCESS, start_scn.convert_for_tx(is_minor && file_idx > 0 ? 200 * file_idx : 1));
  ASSERT_EQ(OB_SUCCESS, end_scn.convert_for_tx(snapshot));
  ASSERT_EQ(OB_SUCCESS, data_desc.init(false, meta_table_schema, ls_id, tablet_id,
      merge, snapshot, DATA_CURRENT_VERSION, false, 0, 0, start_scn, end_scn,
      nullptr, 0, is_minor ? EXEC_MODE_LOCAL : EXEC_MODE_OUTPUT));
  ObSSTableIndexBuilder builder(false);
  data_desc.get_desc().sstable_index_builder_ = &builder;
  ObWholeDataStoreDesc index_desc;
  ASSERT_EQ(OB_SUCCESS, index_desc.gen_index_store_desc(data_desc.get_desc()));
  ASSERT_EQ(OB_SUCCESS, builder.init(index_desc.get_desc()));
  ObMacroBlockWriter writer;
  ObMacroDataSeq sequence(0);
  sequence.set_data_block();
  ObMacroSeqParam sequence_param;
  sequence_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  // Keep SSTable object IDs distinct across files and object-size fixtures.
  sequence_param.start_ = sequence.macro_data_seq_ + 1000 + layout * 100 + merge * 10
      + (OB_DEFAULT_MACRO_BLOCK_SIZE == object_size ? 0 : 1000) + file_idx * 10000;
  ObPreWarmerParam prewarm(MEM_PRE_WARM);
  ASSERT_EQ(OB_SUCCESS, writer.open(data_desc.get_desc(), 0, sequence_param, prewarm, cleaner));
  ObDatumRow input;
  ASSERT_EQ(OB_SUCCESS, input.init(allocator, 9));
  char reference_buf[128];
  int64_t reference_len = 0;
  ASSERT_EQ(OB_SUCCESS, reference.serialize(reference_buf, sizeof(reference_buf), reference_len));
  ObString lob_infos[3];
  for (int64_t i = 0; i < 3; ++i) {
    ObLobMetaExtraInfo extra_info;
    extra_info.extra_info_type_ = i == 2
        ? ObLobMetaExtraInfo::LOB_META_SUMMARY : ObLobMetaExtraInfo::LOB_DATA;
    if (i == 1) {
      extra_info.lob_chunk_type_.set_in_table();
    } else {
      extra_info.lob_chunk_type_.set_out_table();
      extra_info.compressed_len_ = object_size;
      if (i == 0) {
        extra_info.has_crc_ = true;
        extra_info.crc_ = static_cast<uint64_t>(reference.fifth_id());
      }
    }
    ASSERT_EQ(OB_SUCCESS, extra_info.serialize(allocator, lob_infos[i]));
  }
  const int64_t physical_rows = is_minor ? 5 : 4;
  for (int64_t i = 0; i < physical_rows; ++i) {
    const bool deletion = is_minor && i == 0;
    const int64_t data_idx = is_minor ? MAX(0, i - 1) : i;
    const char *lob_ids[] = {"A", "B", "C", "D"};
    input.row_flag_.set_flag(deletion ? DF_DELETE : DF_INSERT);
    input.mvcc_row_flag_.reset();
    input.mvcc_row_flag_.set_first_multi_version_row(!is_minor || i >= 2);
    input.mvcc_row_flag_.set_last_multi_version_row(!is_minor || i != 0);
    input.mvcc_row_flag_.set_compacted_multi_version_row(true);
    input.storage_datums_[0].set_int(0); // lob_col_idx
    input.storage_datums_[1].set_string(lob_ids[data_idx], 1); // lob_id
    input.storage_datums_[2].set_string("1", 1); // seq_id
    input.storage_datums_[3].set_int(100 - snapshot + i); // hidden transaction version
    input.storage_datums_[4].set_int(0); // hidden SQL sequence
    // Simulate a shorter stored length for the small OUTTABLE object.
    const int64_t byte_len = (0 == data_idx && object_size < OB_DEFAULT_MACRO_BLOCK_SIZE)
        ? object_size + 1 : object_size;
    input.storage_datums_[5].set_uint(byte_len);
    input.storage_datums_[6].set_uint(10);
    if (data_idx == 3) { input.storage_datums_[7].set_null(); } // Legacy IN_TABLE.
    else { input.storage_datums_[7].set_string(lob_infos[data_idx]); }
    input.storage_datums_[8].set_string(reference_buf, reference_len);
    if (data_idx >= 2) { input.storage_datums_[8].set_null(); }
    else if (data_idx == 1) { input.storage_datums_[8].set_string("", 0); }
    if (deletion) {
      for (int64_t j = 5; j < 9; ++j) { input.storage_datums_[j].set_nop(); }
    }
    ASSERT_EQ(OB_SUCCESS, writer.append_row(input));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ObSSTableMergeRes res;
  ASSERT_EQ(OB_SUCCESS, builder.close(res));
  ObTabletCreateSSTableParam param;
  ASSERT_EQ(OB_SUCCESS, param.init_for_empty_major_sstable(tablet_id, meta_schema, snapshot, -1, false, !is_minor));
  ASSERT_EQ(OB_SUCCESS, param.inner_init_with_merge_res(res));
  param.nested_size_ = res.nested_size_;
  param.nested_offset_ = res.nested_offset_;
  param.max_merged_trans_version_ = res.max_merged_trans_version_;
  param.min_merged_trans_version_ = res.min_merged_trans_version_;
  if (is_minor) {
    param.table_key_.table_type_ = ObITable::MINOR_SSTABLE;
    param.table_key_.scn_range_.start_scn_ = start_scn;
    param.table_key_.scn_range_.end_scn_ = end_scn;
    param.filled_tx_scn_ = end_scn;
    param.rec_scn_ = start_scn;
  }
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable(param, allocator, handle));
}

// LOB-COL-M011, M012: independent real-file tests, even if SQL history testing fails.
TEST_F(ObSSLocalMinorWatermarkTest, lob_columns_meta_file_layouts)
{
  using namespace blocksstable;
  using namespace share::schema;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObSchemaGetterGuard guard;
  const ObTableSchema *control_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard));
  ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(OB_SYS_TENANT_ID,
      ObString::make_string("oceanbase"), ObString::make_string(share::OB_ALL_OUTTABLE_LOB_CONTROL_TNAME),
      false, control_schema));
  ASSERT_NE(nullptr, control_schema);
  ObArray<ObTabletID> tablet_ids;
  ObArray<share::ObLSID> ls_ids;
  ASSERT_EQ(OB_SUCCESS, control_schema->get_tablet_ids(tablet_ids));
  ASSERT_EQ(1, tablet_ids.count());
  ASSERT_EQ(OB_SUCCESS, share::ObTabletToLSTableOperator::batch_get_ls(
      get_curr_simple_server().get_observer().get_mysql_proxy(), OB_SYS_TENANT_ID, tablet_ids, ls_ids));
  const ObTabletID tablet_id = tablet_ids.at(0);
  MacroBlockId object_a;
  object_a.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
  object_a.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
  object_a.set_second_id(2026101804);
  object_a.set_third_id(tablet_id.id());
  object_a.set_fourth_id(INT64_MAX - 1);
  // M011: write real Meta-layout SSTables with the existing writer. This tests file reading,
  // not SQL LOB allocation or compaction policy. The files are never installed in the live LS.
  const ObRowStoreType layouts[] = {FLAT_ROW_STORE, ENCODING_ROW_STORE};
  const ObMergeType merges[] = {MINOR_MERGE, MAJOR_MERGE};
  for (const auto layout : layouts) {
    for (const auto merge : merges) {
      SCOPED_TRACE(testing::Message() << "Meta layout=" << layout << " merge=" << merge);
      const auto verify_layout = [&](const int64_t object_size) {
        // Use a fresh LOB object ID for each fixture: deleting files does not clear
        // the in-memory macro cache or cancel pending cache persistence.
        object_a.set_fourth_id(object_a.fourth_id() - 1);
        ObArenaAllocator allocator("LobGCFileTest");
        char *source_buf = static_cast<char *>(allocator.alloc(object_size));
        ASSERT_NE(nullptr, source_buf);
        MEMSET(source_buf, 'L', object_size);
        source_buf[0] = 'A';
        source_buf[object_size - 1] = 'Z';
        const ObString source(object_size, source_buf);
        const uint64_t crc = common::ob_crc64(source.ptr(), source.length());
        object_a.set_fifth_id(static_cast<int64_t>(crc));
        ASSERT_TRUE(object_a.is_valid());
        MacroBlockId object_b = object_a;
        object_b.set_second_id(2026101805);
        ASSERT_TRUE(object_b.is_valid());
        ObTableSchema meta_table_schema;
        ASSERT_EQ(OB_SUCCESS, share::ObInnerTableSchema::all_tenant_profile_aux_lob_meta_schema(meta_table_schema));
        meta_table_schema.set_schema_version(100);
        meta_table_schema.set_row_store_type(layout);
        ASSERT_EQ(OB_SUCCESS, meta_table_schema.set_compress_func_name(
            layout == ENCODING_ROW_STORE ? OB_DEFAULT_COMPRESS_FUNC_NAME : "none"));
        ObStorageSchema meta_schema;
        ASSERT_EQ(OB_SUCCESS, meta_schema.init(allocator, meta_table_schema, lib::Worker::CompatMode::MYSQL));
        ASSERT_EQ(3, meta_schema.get_rowkey_column_num());
        const bool is_minor = MINOR_MERGE == merge;
        // Keep both files alive until every Tablet version has been scanned.
        ObSSTablePrivateObjectCleaner cleaners[2];
        char object_buf[128];
        int64_t object_len = 0;
        ASSERT_EQ(OB_SUCCESS, object_a.serialize(object_buf, sizeof(object_buf), object_len));
        ObTableHandleV2 file_handle;
        create_lob_meta_sstable(allocator, meta_table_schema, meta_schema, ls_ids.at(0), tablet_id,
            merge, object_size, 0, object_a, cleaners[0], file_handle);
        ASSERT_FALSE(HasFatalFailure());
        ObTabletTableStore store;
        ObTablet isolated;
        // Borrow test-owned metadata, and detach before the Tablet's destructor runs.
        DEFER({ isolated.storage_schema_addr_.ptr_ = nullptr; isolated.table_store_addr_.ptr_ = nullptr; });
        store.is_inited_ = true;
        isolated.is_inited_ = true;
        isolated.storage_schema_addr_.ptr_ = &meta_schema;
        isolated.table_store_addr_.ptr_ = &store;
        ObSEArray<ObITable *, 1> one_file;
        ASSERT_EQ(OB_SUCCESS, one_file.push_back(file_handle.get_table()));
        if (is_minor) { ASSERT_EQ(OB_SUCCESS, store.minor_tables_.init(allocator, one_file)); }
        else { ASSERT_EQ(OB_SUCCESS, store.major_tables_.init(allocator, one_file)); }
        const ObColumnSchemaV2 *block_column = meta_table_schema.get_column_schema("lob_block_id");
        ASSERT_NE(nullptr, block_column);
        ObSEArray<ObColDesc, 2> requested;
        ObColDesc block_desc;
        block_desc.col_id_ = block_column->get_column_id();
        block_desc.col_type_ = block_column->get_meta_type();
        ASSERT_EQ(OB_SUCCESS, requested.push_back(block_desc));
        const ObDatumRow *output = nullptr;
        ObLobSSTableScanCache scan_cache;
        {
          ObLobColumnIterator stopped;
          ASSERT_EQ(OB_SUCCESS, stopped.init_with_scan_cache(isolated, requested, false, scan_cache));
          ASSERT_EQ(OB_SUCCESS, stopped.get_next_row(output));
          // Stopping after one row must not make the next iterator skip the rest of this file.
        }
        {
          ObLobColumnIterator meta_iter;
          ASSERT_EQ(OB_SUCCESS, meta_iter.init_with_scan_cache(isolated, requested, false, scan_cache));
          int64_t count = 0;
          int ret = OB_SUCCESS;
          while (OB_SUCCESS == (ret = meta_iter.get_next_row(output))) {
            ASSERT_EQ(1, output->count_);
            if (count >= 2) {
              ASSERT_TRUE(output->storage_datums_[0].is_null());
            } else if (count == 1) {
              ASSERT_EQ(0, output->storage_datums_[0].len_);
              ASSERT_FALSE(output->storage_datums_[0].is_nop());
            } else {
              ASSERT_EQ(object_len, output->storage_datums_[0].len_);
              ASSERT_EQ(0, MEMCMP(object_buf, output->storage_datums_[0].ptr_, object_len));
            }
            ++count;
          }
          ASSERT_EQ(OB_ITER_END, ret);
          ASSERT_EQ(nullptr, output);
          ASSERT_EQ(4, count); // Preserve binary, empty and NULL values; skip the absent DELETE value.
        }
        // Capacity fallback is independent of row format and LOB object size.
        if (FLAT_ROW_STORE == layout && !is_minor && OB_DEFAULT_MACRO_BLOCK_SIZE == object_size) {
          ObLobSSTableScanCache new_scan;
          ObITable::TableKey key;
          key.table_type_ = ObITable::MAJOR_SSTABLE;
          key.tablet_id_ = tablet_id;
          for (int64_t i = 0; i < ObLobSSTableScanCache::MAX_SSTABLE_COUNT; ++i) {
            key.version_range_.snapshot_version_ = 1000 + i;
            ASSERT_EQ(OB_SUCCESS, new_scan.add(key));
          }
          ASSERT_EQ(ObLobSSTableScanCache::MAX_SSTABLE_COUNT, new_scan.scanned_sstables_.size());
          ASSERT_FALSE(new_scan.contains(file_handle.get_table()->get_key()));
          ObLobColumnIterator fresh;
          ASSERT_EQ(OB_SUCCESS, fresh.init_with_scan_cache(isolated, requested, false, new_scan));
          int64_t count = 0;
          int ret = OB_SUCCESS;
          while (OB_SUCCESS == (ret = fresh.get_next_row(output))) {
            ASSERT_NE(nullptr, output);
            ASSERT_EQ(1, output->count_);
            if (0 == count) {
              ASSERT_EQ(object_len, output->storage_datums_[0].len_);
              ASSERT_EQ(0, MEMCMP(object_buf, output->storage_datums_[0].ptr_, object_len));
            }
            ++count;
          }
          ASSERT_EQ(OB_ITER_END, ret);
          ASSERT_EQ(nullptr, output);
          ASSERT_EQ(4, count);
          ASSERT_EQ(ObLobSSTableScanCache::MAX_SSTABLE_COUNT, new_scan.scanned_sstables_.size());
          ASSERT_FALSE(new_scan.contains(file_handle.get_table()->get_key()));
        }
        if (OB_DEFAULT_MACRO_BLOCK_SIZE == object_size) {
          // V1 contains S1 -> A; V2 reuses S1 and adds S2 -> B.
          // Major skips S1; Minor rereads both files.
          ObTableHandleV2 second_file;
          create_lob_meta_sstable(allocator, meta_table_schema, meta_schema, ls_ids.at(0), tablet_id,
              merge, object_size, 1, object_b, cleaners[1], second_file);
          ASSERT_FALSE(HasFatalFailure());
          ObTabletTableStore next_store;
          ObTablet next_version;
          DEFER({ next_version.storage_schema_addr_.ptr_ = nullptr; next_version.table_store_addr_.ptr_ = nullptr; });
          next_store.is_inited_ = true;
          next_version.is_inited_ = true;
          next_version.storage_schema_addr_.ptr_ = &meta_schema;
          next_version.table_store_addr_.ptr_ = &next_store;
          ObSEArray<ObITable *, 2> version_files;
          ASSERT_EQ(OB_SUCCESS, version_files.push_back(file_handle.get_table()));
          ASSERT_EQ(OB_SUCCESS, version_files.push_back(second_file.get_table()));
          if (is_minor) { ASSERT_EQ(OB_SUCCESS, next_store.minor_tables_.init(allocator, version_files)); }
          else { ASSERT_EQ(OB_SUCCESS, next_store.major_tables_.init(allocator, version_files)); }
          ASSERT_EQ(is_minor ? 0 : 1, scan_cache.scanned_sstables_.size());
          ASSERT_EQ(!is_minor, scan_cache.contains(file_handle.get_table()->get_key()));
          ASSERT_FALSE(scan_cache.contains(second_file.get_table()->get_key()));
          ObLobColumnIterator next_iter;
          ASSERT_EQ(OB_SUCCESS, next_iter.init_with_scan_cache(next_version, requested, false, scan_cache));
          int64_t count = 0;
          int ret = OB_SUCCESS;
          while (OB_SUCCESS == (ret = next_iter.get_next_row(output))) {
            ASSERT_NE(nullptr, output);
            ASSERT_EQ(1, output->count_);
            const ObStorageDatum &datum = output->storage_datums_[0];
            const int64_t row_idx = count % 4;
            if (0 == row_idx) {
              MacroBlockId actual;
              int64_t pos = 0;
              ASSERT_EQ(OB_SUCCESS, actual.deserialize(datum.ptr_, datum.len_, pos));
              ASSERT_EQ(datum.len_, pos);
              ASSERT_EQ(is_minor && count < 4 ? object_a : object_b, actual);
            }
            ++count;
          }
          ASSERT_EQ(OB_ITER_END, ret);
          ASSERT_EQ(nullptr, output);
          ASSERT_EQ(is_minor ? 8 : 4, count);
          ASSERT_EQ(is_minor ? 0 : 2, scan_cache.scanned_sstables_.size());
          ASSERT_EQ(!is_minor, scan_cache.contains(second_file.get_table()->get_key()));
        }
        // M012: DELETE keeps the key but has NOP for lob_block_id; reject this partial-NOP row.
        if (is_minor) {
          ObColDesc key_desc;
          key_desc.col_id_ = OB_APP_MIN_COLUMN_ID;
          key_desc.col_type_ = meta_table_schema.get_column_schema(OB_APP_MIN_COLUMN_ID)->get_meta_type();
          ASSERT_EQ(OB_SUCCESS, requested.push_back(key_desc));
          ObLobColumnIterator meta_iter;
          ASSERT_EQ(OB_SUCCESS, meta_iter.init(isolated, requested, false));
          ASSERT_EQ(OB_ERR_UNEXPECTED, meta_iter.get_next_row(output));
          ASSERT_EQ(nullptr, output);
        }
        ASSERT_NO_FATAL_FAILURE(
            verify_lob_physical_backup_restore(isolated, object_a, ls_ids.at(0), source));
      };
      const int64_t object_sizes[] = {OB_DEFAULT_MACRO_BLOCK_SIZE, 8193};
      for (const int64_t object_size : object_sizes) {
        SCOPED_TRACE(testing::Message() << "object_size=" << object_size);
        verify_layout(object_size);
      }
    }
  }
}

TEST_F(ObSSLocalMinorWatermarkTest, end)
{
  ObSqlString sql;
  int64_t affected_rows = 0;
  // 兜底清理：若 normal 用例中途 ASSERT 退出，也要关闭 tracepoint 并删除测试表。
  SYS_EXE_SQL("alter system set_tp tp_name = LS_SS_INC_UPLOAD_SKIP_REGISTER, error_code = 4016, frequency = 0;");
  EXE_SQL("drop table if exists test_table;");
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
  char *log_level = (char *)"INFO";
  // Keep farm-provided defaults, and allow a standalone mittest machine to supply its S3 endpoint.
  const auto env_or_default = [](const char *name, const char *default_value) {
    const char *value = std::getenv(name);
    return nullptr != value && '\0' != value[0] ? value : default_value;
  };
  const char *s3_bucket = env_or_default("OB_MITTEST_S3_BUCKET", oceanbase::unittest::S3_BUCKET);
  const char *s3_endpoint = env_or_default("OB_MITTEST_S3_ENDPOINT", oceanbase::unittest::S3_ENDPOINT);
  const char *s3_ak = env_or_default("OB_MITTEST_S3_AK", oceanbase::unittest::S3_AK);
  const char *s3_sk = env_or_default("OB_MITTEST_S3_SK", oceanbase::unittest::S3_SK);
  const char *s3_region = env_or_default("OB_MITTEST_S3_REGION", oceanbase::unittest::S3_REGION);
  char buf[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  const int format_ret = databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      s3_bucket, cur_time_ns, s3_endpoint, s3_ak, s3_sk, s3_region);
  if (OB_SUCCESS != format_ret) {
    return format_ret;
  }
  oceanbase::shared_storage_info = buf;
  while (EOF != (c = getopt(argc, argv, "t:l:"))) {
    switch (c) {
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
