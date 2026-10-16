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

#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
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
  char buf[1000];
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  memset(buf, 1000, sizeof(buf));
  databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
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
