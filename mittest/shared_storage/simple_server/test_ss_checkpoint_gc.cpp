/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <gmock/gmock.h>

#define protected public
#define private public
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_defer.h"
#include "lib/utility/ob_tracepoint.h"
#include "logservice/palf/lsn.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "src/storage/shared_storage/ob_remote_io_adapter.h"
#include "src/storage/shared_storage/ob_ss_object_access_util.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "src/storage/incremental/ob_shared_meta_service.h"
#include "src/storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "src/storage/incremental/ssmeta/ob_ss_meta_base_operator.h"
#include "src/storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "src/storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "src/storage/ss_garbage_collector/task/ob_ss_ckpt_gc_task.h"
#include "src/storage/ss_garbage_collector/task/ob_ss_fail_gc_gen_task.h"
#include "src/storage/ss_garbage_collector/task/ob_ss_mark_sweep_gc_gen_task.h"
#include "src/storage/ss_garbage_collector/ob_ss_gc_task_runner.h"
#include "src/storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_util.h"
#include "src/storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"

namespace oceanbase
{
char *shared_storage_info = nullptr;

OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT

using namespace unittest;
namespace storage
{
using namespace common;
using namespace share;

// 保留真实 MetaService 位点读取；本环境未开启 logservice，应在对象读取前返回失败。
class TestableSSMarkSweepGCGenTask : public ObSSMarkSweepGCGenTask
{
public:
  explicit TestableSSMarkSweepGCGenTask(volatile bool &is_stopped)
    : ObSSMarkSweepGCGenTask(is_stopped),
      read_count_(0)
  {}

  int64_t read_count() const { return read_count_; }

protected:
  int read_ls_ckpt_data_(
      const SCN &ckpt_meta_scn,
      ObSSLSMeta &ls_meta,
      ObIArray<blocksstable::MacroBlockId> &tablet_meta_keys) override
  {
    UNUSED(ckpt_meta_scn);
    ++read_count_;
    ls_meta.reset();
    tablet_meta_keys.reset();
    return OB_OBJECT_NOT_EXIST;
  }

private:
  int64_t read_count_;
};

// 只替换外部读取，保留 read_ls_ckpt_with_retry_ 的重试、代际检查和出参提交逻辑。
class MockSSMarkSweepGCGenTask : public ObSSMarkSweepGCGenTask
{
public:
  explicit MockSSMarkSweepGCGenTask(volatile bool &is_stopped)
    : ObSSMarkSweepGCGenTask(is_stopped),
      success_ckpt_meta_scn_(),
      missing_ss_checkpoint_scn_(),
      success_ss_checkpoint_scn_(),
      missing_tablet_meta_key_(),
      success_tablet_meta_key_(),
      second_success_tablet_meta_key_()
  {}
  ~MockSSMarkSweepGCGenTask() override {}

  MOCK_METHOD1(get_ls_ckpt_meta_scn_, int(SCN &ckpt_meta_scn));
  MOCK_METHOD3(read_ls_ckpt_data_,
      int(const SCN &ckpt_meta_scn, ObSSLSMeta &ls_meta,
          ObIArray<blocksstable::MacroBlockId> &tablet_meta_keys));

  int fill_ls_ckpt_data(
      const SCN &ckpt_meta_scn,
      ObSSLSMeta &ls_meta,
      ObIArray<blocksstable::MacroBlockId> &tablet_meta_keys)
  {
    int ret = OB_SUCCESS;
    const bool read_success = ckpt_meta_scn == success_ckpt_meta_scn_;
    EXPECT_TRUE(tablet_meta_keys.empty());
    ls_meta.ss_checkpoint_scn_ = read_success
        ? success_ss_checkpoint_scn_
        : missing_ss_checkpoint_scn_;
    if (OB_FAIL(tablet_meta_keys.push_back(
        read_success ? success_tablet_meta_key_ : missing_tablet_meta_key_))) {
      LOG_WARN("fail to fill mocked checkpoint tablet meta key", KR(ret));
    } else if (read_success) {
      if (OB_FAIL(tablet_meta_keys.push_back(second_success_tablet_meta_key_))) {
        LOG_WARN("fail to fill second mocked checkpoint tablet meta key", KR(ret));
      }
    } else {
      // 失败前故意写入旧代内容，验证重试不会把部分结果带到最终出参。
      ret = OB_OBJECT_NOT_EXIST;
    }
    return ret;
  }

private:
  static __thread MockSSMarkSweepGCGenTask *active_mock_;
  SCN success_ckpt_meta_scn_;
  SCN missing_ss_checkpoint_scn_;
  SCN success_ss_checkpoint_scn_;
  blocksstable::MacroBlockId missing_tablet_meta_key_;
  blocksstable::MacroBlockId success_tablet_meta_key_;
  blocksstable::MacroBlockId second_success_tablet_meta_key_;

  DISALLOW_COPY_AND_ASSIGN(MockSSMarkSweepGCGenTask);
};

// 与本文件已有 PALF mock 一样，通过测试可执行文件覆盖共享库符号。
// 仅当前测试线程的作用域内使用脚本，其余调用继续经过真实 MetaService。
__thread MockSSMarkSweepGCGenTask *MockSSMarkSweepGCGenTask::active_mock_ = nullptr;

int ObSSGCUtil::get_ls_ckpt_meta_scn_from_palf_kv(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    SCN &ckpt_meta_scn)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
  ckpt_meta_scn.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_NOT_NULL(MockSSMarkSweepGCGenTask::active_mock_)
      && is_sys_tenant(tenant_id) && is_sys_tenant(MTL_ID()) && ls_id.is_sys_ls()) {
    ret = MockSSMarkSweepGCGenTask::active_mock_->get_ls_ckpt_meta_scn_(ckpt_meta_scn);
  } else if (OB_ISNULL(meta_srv)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = meta_srv->get_ls_ckpt_meta_scn(ls_id, ckpt_meta_scn);
  }
  return ret;
}

// 测试策略（checkpoint GC 基础契约与 MarkSweep 保留路径）：
//   1. fixture SetUp：按需创建 tt1 user 租户，保证 filter/shuffle 下测试独立。
//   3. forge_and_batch_del_e2e：伪造真实 shared_meta LS_CKPT 行（in_table macro_id → path），
//      调 get_ls_ckpt_iter → meta_srv->batch_del_ls_ckpt  走完真链路；round-trip 后行被删。
//   4. batch_del_idempotent：对同一 meta_scn 重复调用不报错（含"行已删、对象残留"等价场景）。
//   5. DISABLED_gen_need_generate_task_checkpoint_false：CHECKPOINT ctrl 行不生成 FAIL_GC 任务，当前禁用。
//   6. batch_delete_replays_partially_deleted_batch：模拟批内前缀已删，整批重放后补删剩余项。
//   7. gc_isolation_between_ls：底层批删接口只影响目标 LS，不误删其他 LS。
//   8. batch_del_ckpts_argument_validation / batch_del_rejected_on_user_tenant / batch_del_invalid_scn：
//      参数与 sys-only 校验，保证不误删。
//   9. batch_del_removes_real_object：伪造真实 OSS 对象 + inner table 行，验证双删把行 + 对象
//      都删净（唯一真正走到 OB_STORAGE_OBJECT_MGR.delete_object 的用例）。
//  10. batch_del_reclaims_orphan_object：模拟"行已删、对象残留"，再次调 batch_del 补删对象且成功
//      —— 覆盖 §3.4 不依赖 MarkSweep 兜底的幂等补删。
//  11. ckpt_scn_domains_separated_and_merge_uses_ckpt_meta_scn：checkpoint 对象定位位点、
//      LS 数据位点、Tablet 精确引用位点分域，且 merge 采用 checkpoint 的精确位点。
//  12. runner_starts_ckpt_gc_only_on_sys：ObSSGCTaskRunner::start_ckpt_gc_task_ 对 sys 启动、
//      对 user 跳过（commit 语义 "Checkpoint GC starts in sys tenant only"）。
//  13. filter_uses_effective_ls_checkpoint_scn_map：按传入的 LS 数据位点保留或回退 Tablet 位点；
//      本用例不读取 PALF KV。
//  14. merge_ckpt_skips_tablet_newer_than_snapshot：checkpoint 引用晚于快照且 study 缺失时跳过。
//  15. merge_ckpt_missing_visible_tablet_returns_eagain：checkpoint 引用已进入快照但 study 缺失时重试。
//
//  16. read_ckpt_fails_fast_and_clears_output：位点读取失败即清空出参且不读对象。
//
// ============================ 覆盖边界 ============================
// 本 simple_server 环境未开启 logservice，真实位点读取返回 OB_NOT_SUPPORTED。
// read_ckpt_fails_fast_and_clears_output 经真实 MetaService 验证失败，不激活脚本 mock。
// read_ckpt_retries_with_new_generation / read_ckpt_retry_exhaustion_clears_output
// 使用脚本化读取验证重试状态机：每轮重读 PALF 位点、成功出参属于同代、失败出参全清空。
// 测试专用符号覆盖不验证 Util 故障点；脚本也不覆盖实际 PALF KV、对象存储或反序列化。
// 真实读取接线由 obtest 1108 用例验证，生产 generator 的头文件和实现无需增加接缝。
// 以下路径仍未由本文件验证：PALF KV 无 key、非法 MacroBlockId、正常反序列化、
// 位点回退、已见发布版本后无 key，以及缺 key 时调用方继续完成 MarkSweep DAG。
// LS_CKPT_GC 生成端测试直接传入发布版本，仅验证回收边界和任务表写入。
class TestSSCheckpointGC : public ObSimpleClusterTestBase
{
public:
  TestSSCheckpointGC()
    : ObSimpleClusterTestBase("test_ss_checkpoint_gc", "50G", "50G", "50G") {}

  static uint64_t tenant_id_;

  void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    if (!is_user_tenant(tenant_id_)) {
      ASSERT_EQ(OB_SUCCESS, create_tenant("tt1", "5G", "10G", false, 10));
      ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
      ASSERT_TRUE(is_user_tenant(tenant_id_));
      ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());

      bool tenant_ready = false;
      const int64_t deadline = ObTimeUtility::current_time() + 120LL * 1000000LL;
      while (!tenant_ready && ObTimeUtility::current_time() < deadline) {
        share::ObTenantSwitchGuard guard;
        if (OB_SUCCESS == guard.switch_to(tenant_id_)
            && OB_NOT_NULL(MTL(ObLSService *))) {
          tenant_ready = true;
        } else {
          ob_usleep(10 * 1000);
        }
      }
      ASSERT_TRUE(tenant_ready);
    }
  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

  static SCN make_scn(const int64_t val)
  {
    SCN scn;
    EXPECT_EQ(OB_SUCCESS, scn.convert_for_inner_table_field(static_cast<uint64_t>(val)));
    return scn;
  }

  // 伪造一行 LS_CKPT：表内 key 与 11b4 提供的 CHECKPOINT 原子协议保持一致。
  static int insert_forged_ls_ckpt_row(const ObLSID &ls_id, const SCN &meta_scn)
  {
    int ret = OB_SUCCESS;
    ObSSMetaLSCkptKey in_table_key;
    in_table_key.ls_id_ = ls_id;
    in_table_key.meta_scn_ = meta_scn;
    blocksstable::MacroBlockId in_table_id = in_table_key.convert_to_macro_id(true/*is_in_table*/);
    char path_buf[SSMETA_PATH_MAX_LEN] = {0};
    int64_t pos = 0;
    if (OB_FAIL(blocksstable::SSObjUtil::to_relative_remote_path_format(
            in_table_id, path_buf, sizeof(path_buf), pos))) {
      LOG_WARN("format path failed", KR(ret), K(in_table_id));
    } else {
      ObSSMetaTableOperator op;
      ObSSMetaType meta_type;
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);
      const ObString meta_key(static_cast<int32_t>(pos), path_buf);
      // meta_value 用最小占位内容 —— GC 端只关心 meta_scn，不解析 value。
      const char *placeholder = "forged_ls_ckpt_value";
      const ObString meta_value(static_cast<int32_t>(STRLEN(placeholder)), placeholder);
      int64_t affected_rows = 0;
      if (OB_FAIL(op.init(*GCTX.sql_proxy_, OB_ALL_SHARED_META_TNAME))) {
        LOG_WARN("op init failed", KR(ret));
      } else if (OB_FAIL(op.insert_row(MTL_ID(), ls_id, meta_type, meta_key, meta_value, affected_rows))) {
        LOG_WARN("insert forged ls ckpt row failed", KR(ret), K(ls_id), K(meta_scn));
      }
    }
    return ret;
  }

  // 遍历 __all_shared_meta 中 (sys, ls_id) 下 meta_scn <= read_snapshot 的所有 LS_CKPT 行，
  // 收集 meta_scn。read_snapshot 默认为 max_scn（全量）。
  static int list_ls_ckpt_meta_scns(
      const ObLSID &ls_id,
      ObIArray<SCN> &meta_scns,
      const SCN &read_snapshot = SCN::max_scn())
  {
    int ret = OB_SUCCESS;
    meta_scns.reuse();
    ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
    if (OB_ISNULL(meta_srv)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObSSMetaIterGuard<ObSSMetaLSCkptKeyIter> guard;
      ObSSMetaLSCkptKeyIter *iter = nullptr;
      if (OB_FAIL(meta_srv->get_ls_ckpt_iter(ls_id, read_snapshot,
          true/*read_mark_delete*/, guard))) {
        LOG_WARN("get_ls_ckpt_iter failed", KR(ret), K(ls_id), K(read_snapshot));
      } else if (OB_FAIL(guard.get_iter(iter))) {
        LOG_WARN("get iter failed", KR(ret));
      } else {
        ObSSMetaLSCkptKey key;
        while (OB_SUCC(ret)) {
          key.reset();
          int tmp_ret = iter->get_next(key);
          if (OB_ITER_END == tmp_ret) {
            break;
          } else if (OB_SUCCESS != tmp_ret) {
            ret = tmp_ret;
          } else {
            ret = meta_scns.push_back(key.meta_scn_);
          }
        }
      }
    }
    return ret;
  }

  // 写入一个真实对象存储对象（内容占位），用于覆盖 batch_del_ls_ckpt 的对象删除半边。
  static int write_object_(const blocksstable::MacroBlockId &macro_id)
  {
    int ret = OB_SUCCESS;
    static char write_buf[4096];
    memset(write_buf, 'c', sizeof(write_buf));
    blocksstable::ObStorageObjectWriteInfo write_info;
    write_info.io_desc_.set_wait_event(1);
    write_info.buffer_ = write_buf;
    write_info.offset_ = 0;
    write_info.size_ = sizeof(write_buf);
    write_info.io_timeout_ms_ = 60 * 1000; // 60s
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.write_strategy_ = blocksstable::ObStorageObjectWriteStrategy::WRITE_THROUGH;

    blocksstable::ObStorageObjectHandle write_object_handle;
    if (OB_FAIL(write_object_handle.set_macro_block_id(macro_id))) {
      LOG_WARN("fail to set macro block id", KR(ret), K(macro_id));
    } else if (OB_FAIL(ObSSObjectAccessUtil::async_write_file(write_info, write_object_handle))) {
      LOG_WARN("fail to async write object", KR(ret), K(macro_id));
    } else if (OB_FAIL(write_object_handle.wait())) {
      LOG_WARN("fail to wait object write", KR(ret), K(macro_id));
    }
    return ret;
  }

  // 断言对象存储中对象是否存在。须在目标租户上下文中调用。
  static void assert_object_exists_(const blocksstable::MacroBlockId &macro_id,
                                    const bool expected_exist)
  {
    bool actual = false;
    ASSERT_EQ(OB_SUCCESS, ObRemoteIoAdapter::is_exist(MTL_ID(), macro_id, 0/*ls_epoch_id*/, actual));
    char id_buf[256] = {0};
    macro_id.to_string(id_buf, sizeof(id_buf));
    ASSERT_EQ(expected_exist, actual) << "macro_id=" << id_buf;
  }

  // 生成 LS_CKPT 的对象存储 macro_id（is_in_table=false），与 batch_del_ls_ckpt 删对象所用一致。
  static blocksstable::MacroBlockId ls_ckpt_oss_macro_id(const ObLSID &ls_id, const SCN &meta_scn)
  {
    ObSSMetaLSCkptKey ckpt_key;
    ckpt_key.ls_id_ = ls_id;
    ckpt_key.meta_scn_ = meta_scn;
    return ckpt_key.convert_to_macro_id(false/*is_in_table*/);
  }

  // 伪造完整的一行 LS_CKPT：inner table 行 + 真实对象存储对象。
  // 与仅插行的 insert_forged_ls_ckpt_row 不同，本函数让 batch_del_ls_ckpt 的对象删除半边
  // 真正有对象可删（否则对象恒不存在，等价于"对象已删 → 视为成功"，覆盖不到实际删对象）。
  static int insert_forged_ls_ckpt_with_object(const ObLSID &ls_id, const SCN &meta_scn)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(insert_forged_ls_ckpt_row(ls_id, meta_scn))) {
      LOG_WARN("insert forged row failed", KR(ret), K(ls_id), K(meta_scn));
    } else if (OB_FAIL(write_object_(ls_ckpt_oss_macro_id(ls_id, meta_scn)))) {
      LOG_WARN("write forged ls ckpt object failed", KR(ret), K(ls_id), K(meta_scn));
    }
    return ret;
  }

  // 清掉 (sys, ls_id) 下全部残留 LS_CKPT 行（测试隔离）。须在 sys 上下文调用。
  static int clear_ls_ckpt_rows(const ObLSID &ls_id)
  {
    int ret = OB_SUCCESS;
    ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
    ObSEArray<SCN, 256> scns;
    if (OB_ISNULL(meta_srv)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(list_ls_ckpt_meta_scns(ls_id, scns))) {
      LOG_WARN("list ls ckpt failed", KR(ret), K(ls_id));
    } else if (scns.count() > 0 && OB_FAIL(meta_srv->batch_del_ls_ckpt(ls_id, scns))) {
      LOG_WARN("batch del ls ckpt failed", KR(ret), K(ls_id));
    }
    return ret;
  }

  static int find_ls_ckpt_gc_task(
      const ObIArray<ObSharedGCTask> &gc_tasks,
      const SCN &meta_scn,
      ObSharedGCTask &target_task,
      int64_t &match_count)
  {
    int ret = OB_SUCCESS;
    match_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < gc_tasks.count(); ++i) {
      ObSharedGCLSCheckpointInfo gc_info;
      if (OB_FAIL(gc_info.from_value(gc_tasks.at(i).value_))) {
      } else if (gc_info.ls_id_ == SYS_LS && gc_info.meta_scn_ == meta_scn) {
        target_task = gc_tasks.at(i);
        ++match_count;
      }
    }
    return ret;
  }
};

uint64_t TestSSCheckpointGC::tenant_id_ = OB_INVALID_TENANT_ID;

// 伪造真实 shared_meta LS_CKPT 行，跑 get_ls_ckpt_iter → meta_srv->batch_del_ls_ckpt 真链路，
// round-trip 后行应被删除。batch_del_ls_ckpt 只允许 sys 租户，故整段在 sys 下执行。
TEST_F(TestSSCheckpointGC, forge_and_batch_del_e2e)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));

  volatile bool stopped = false;
  ObSSCkptGCTask task(stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));

  const ObLSID ls_id(1500);
  ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_srv);

  // 清理上一轮残留（防止测试间干扰）。
  ObSEArray<SCN, 16> pre_scns;
  ASSERT_EQ(OB_SUCCESS, list_ls_ckpt_meta_scns(ls_id, pre_scns));
  if (pre_scns.count() > 0) {
    ASSERT_EQ(OB_SUCCESS, meta_srv->batch_del_ls_ckpt(ls_id, pre_scns));
  }

  // 伪造 3 行 LS_CKPT。
  const int64_t kRows = 3;
  ObSEArray<SCN, kRows> forged;
  for (int64_t i = 0; i < kRows; ++i) {
    const SCN scn = make_scn(1000 + i);
    ASSERT_EQ(OB_SUCCESS, forged.push_back(scn));
    ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_row(ls_id, scn));
  }

  // iter 应能读到伪造的 3 行。
  ObSEArray<SCN, kRows> before;
  ASSERT_EQ(OB_SUCCESS, list_ls_ckpt_meta_scns(ls_id, before));
  ASSERT_EQ(kRows, before.count());

  // meta_srv->batch_del_ls_ckpt 走真 batch_del_ls_ckpt 双删（对象不存在 → 视为成功；行会被删）。
  ASSERT_EQ(OB_SUCCESS, meta_srv->batch_del_ls_ckpt(ls_id, forged));

  // round-trip：iter 再读应为空。
  ObSEArray<SCN, kRows> after;
  ASSERT_EQ(OB_SUCCESS, list_ls_ckpt_meta_scns(ls_id, after));
  ASSERT_EQ(0, after.count());

  task.destroy();
}

TEST_F(TestSSCheckpointGC, ls_ckpt_gc_task_consume_to_history)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_FALSE(GCONF.enable_logservice);

  const SCN meta_scn = make_scn(ObTimeUtility::current_time_ns());
  ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_row(SYS_LS, meta_scn));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskUtil::generate_ls_ckpt_gc_task(
      OB_SYS_TENANT_ID, SYS_LS, meta_scn, *GCTX.sql_proxy_));

  volatile bool stopped = false;
  ObSSCkptGCTask ckpt_gc_task(stopped);
  ASSERT_EQ(OB_SUCCESS, ckpt_gc_task.init(OB_SYS_TENANT_ID));
  ObArenaAllocator allocator("CkptGCConsume");
  ObArray<ObSharedGCTask> gc_tasks;
  ASSERT_EQ(OB_SUCCESS, ckpt_gc_task.get_ls_ckpt_gc_infos_(allocator, gc_tasks));
  ObSharedGCTask target_task;
  int64_t match_count = 0;
  ASSERT_EQ(OB_SUCCESS, find_ls_ckpt_gc_task(gc_tasks, meta_scn, target_task, match_count));
  ASSERT_EQ(1, match_count);

  ObSEArray<ObSharedGCTask, 1> target_tasks;
  ASSERT_EQ(OB_SUCCESS, target_tasks.push_back(target_task));
  ASSERT_EQ(OB_SUCCESS, ckpt_gc_task.do_ls_ckpt_gc_(SYS_LS, target_tasks));

  ObSEArray<SCN, 16> remaining_scns;
  ASSERT_EQ(OB_SUCCESS, list_ls_ckpt_meta_scns(SYS_LS, remaining_scns));
  for (int64_t i = 0; i < remaining_scns.count(); ++i) {
    ASSERT_NE(meta_scn, remaining_scns.at(i));
  }
  ObSharedGCTask history_task;
  ObArenaAllocator history_allocator("CkptGCHistory");
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      target_task.key_, history_allocator, *GCTX.sql_proxy_, history_task));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
      ObSharedGCTaskTableOperator::delete_task(target_task.key_, *GCTX.sql_proxy_));
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::delete_history(target_task.key_, *GCTX.sql_proxy_));
  ckpt_gc_task.destroy();
}

TEST_F(TestSSCheckpointGC, generate_ls_ckpt_gc_task_uses_min_meta_scn_and_deduplicates)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, clear_ls_ckpt_rows(SYS_LS));

  volatile bool stopped = false;
  ObSSMarkSweepGCGenTask gen_task(stopped);
  ASSERT_EQ(OB_SUCCESS, gen_task.init(OB_SYS_TENANT_ID));
  SCN generated_max_scn;
  ASSERT_EQ(OB_SUCCESS, gen_task.calc_gc_ls_ckpt_max_scn_(generated_max_scn));
  const int64_t base_scn = MAX(
      static_cast<int64_t>(generated_max_scn.get_val_for_inner_table_field()) + 1,
      ObTimeUtility::current_time_ns());
  const SCN meta_scn_1 = make_scn(base_scn);
  const SCN meta_scn_2 = make_scn(base_scn + 1);
  const SCN min_meta_scn = make_scn(base_scn + 2);
  const SCN snapshot_meta_scn = make_scn(base_scn + 3);
  ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_row(SYS_LS, meta_scn_1));
  ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_row(SYS_LS, meta_scn_2));
  ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_row(SYS_LS, min_meta_scn));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskUtil::generate_ls_ckpt_gc_task(
      OB_SYS_TENANT_ID, SYS_LS, meta_scn_1, *GCTX.sql_proxy_));

  ObSEArray<SCN, 1> reserved_meta_scns;
  ASSERT_EQ(OB_SUCCESS, gen_task.generate_ls_ckpt_gc_task_(
      snapshot_meta_scn, min_meta_scn, SCN::max_scn(), reserved_meta_scns));
  ASSERT_EQ(OB_SUCCESS, gen_task.generate_ls_ckpt_gc_task_(
      snapshot_meta_scn, min_meta_scn, SCN::max_scn(), reserved_meta_scns));

  ObSSCkptGCTask ckpt_gc_task(stopped);
  ASSERT_EQ(OB_SUCCESS, ckpt_gc_task.init(OB_SYS_TENANT_ID));
  ObArenaAllocator allocator("CkptGCProduce");
  ObArray<ObSharedGCTask> gc_tasks;
  ASSERT_EQ(OB_SUCCESS, ckpt_gc_task.get_ls_ckpt_gc_infos_(allocator, gc_tasks));
  ObSharedGCTask target_task;
  int64_t match_count = 0;
  ASSERT_EQ(OB_SUCCESS, find_ls_ckpt_gc_task(gc_tasks, meta_scn_1, target_task, match_count));
  ASSERT_EQ(1, match_count);
  ASSERT_EQ(OB_SUCCESS, find_ls_ckpt_gc_task(gc_tasks, meta_scn_2, target_task, match_count));
  ASSERT_EQ(1, match_count);
  ASSERT_EQ(OB_SUCCESS, find_ls_ckpt_gc_task(gc_tasks, min_meta_scn, target_task, match_count));
  ASSERT_EQ(0, match_count);

  for (int64_t i = 0; i < gc_tasks.count(); ++i) {
    ObSharedGCLSCheckpointInfo gc_info;
    ASSERT_EQ(OB_SUCCESS, gc_info.from_value(gc_tasks.at(i).value_));
    if (gc_info.meta_scn_ == meta_scn_1 || gc_info.meta_scn_ == meta_scn_2) {
      ASSERT_EQ(OB_SUCCESS,
          ObSharedGCTaskTableOperator::delete_task(gc_tasks.at(i).key_, *GCTX.sql_proxy_));
    }
  }
  ASSERT_EQ(OB_SUCCESS, clear_ls_ckpt_rows(SYS_LS));
  ckpt_gc_task.destroy();
  gen_task.destroy();
}

TEST_F(TestSSCheckpointGC, generate_ls_ckpt_gc_task_respects_published_meta_scn)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, clear_ls_ckpt_rows(SYS_LS));

  volatile bool stopped = false;
  ObSSMarkSweepGCGenTask gen_task(stopped);
  ObSSCkptGCTask ckpt_gc_task(stopped);
  SCN generated_max_scn;
  ObArenaAllocator allocator("CkptGCBound");
  ObArray<ObSharedGCTask> gc_tasks;
  ObSharedGCTask target_task;
  ObSharedGCLSCheckpointInfo gc_info;
  ObSEArray<SCN, 1> reserved_meta_scns;
  int64_t match_count = 0;
  ASSERT_EQ(OB_SUCCESS, gen_task.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, ckpt_gc_task.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, gen_task.calc_gc_ls_ckpt_max_scn_(generated_max_scn));
  const int64_t base_scn = MAX(
      static_cast<int64_t>(generated_max_scn.get_val_for_inner_table_field()) + 1,
      ObTimeUtility::current_time_ns());
  const SCN old_scn = make_scn(base_scn);
  const SCN published_scn = make_scn(base_scn + 2);
  const SCN failed_scn = make_scn(base_scn + 4);
  const SCN safe_meta_scn = make_scn(base_scn + 5);
  const SCN snapshot_meta_scn = make_scn(base_scn + 6);
  // ASSERT 提前退出也要清理；重新查询任务，避免遗漏已生成但尚未读取的任务。
  DEFER({
    int cleanup_ret = OB_SUCCESS;
    gc_tasks.reuse();
    target_task.reset();
    allocator.reuse();
    cleanup_ret = ckpt_gc_task.get_ls_ckpt_gc_infos_(allocator, gc_tasks);
    EXPECT_EQ(OB_SUCCESS, cleanup_ret);
    for (int64_t i = 0; OB_SUCCESS == cleanup_ret && i < gc_tasks.count(); ++i) {
      int tmp_ret = gc_info.from_value(gc_tasks.at(i).value_);
      EXPECT_EQ(OB_SUCCESS, tmp_ret);
      if (OB_SUCCESS == tmp_ret && gc_info.ls_id_ == SYS_LS
          && (gc_info.meta_scn_ == old_scn || gc_info.meta_scn_ == published_scn
              || gc_info.meta_scn_ == failed_scn)) {
        tmp_ret = ObSharedGCTaskTableOperator::delete_task(gc_tasks.at(i).key_, *GCTX.sql_proxy_);
        EXPECT_TRUE(OB_SUCCESS == tmp_ret || OB_ENTRY_NOT_EXIST == tmp_ret) << tmp_ret;
      }
    }
    EXPECT_EQ(OB_SUCCESS, clear_ls_ckpt_rows(SYS_LS));
  });
  ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_row(SYS_LS, old_scn));
  ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_row(SYS_LS, published_scn));
  ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_row(SYS_LS, failed_scn));

  // 无效发布版本报错且不生成任务；最小有效版本成功但不回收。
  // 发布版本自身及更大的失败版本均保留，更旧版本可回收。
  // 推进发布版本后，安全位点和保留位点继续生效；最后验证去重及 MAX 的兼容行为。
  const int64_t case_count = 9;
  const SCN published_meta_scns[case_count] = {
      SCN(), SCN::min_scn(), published_scn, published_scn,
      failed_scn, failed_scn, failed_scn, failed_scn, SCN::max_scn()};
  const SCN safe_scns[case_count] = {
      safe_meta_scn, safe_meta_scn, safe_meta_scn, safe_meta_scn,
      published_scn, safe_meta_scn, safe_meta_scn, safe_meta_scn, safe_meta_scn};
  const SCN reserved_scns[case_count] = {
      safe_meta_scn, safe_meta_scn, safe_meta_scn, safe_meta_scn,
      safe_meta_scn, published_scn, safe_meta_scn, safe_meta_scn, SCN()};
  const int expected_rets[case_count] = {
      OB_ERR_UNEXPECTED, OB_SUCCESS, OB_SUCCESS, OB_SUCCESS, OB_SUCCESS,
      OB_SUCCESS, OB_SUCCESS, OB_SUCCESS, OB_SUCCESS};
  const int64_t old_task_counts[case_count] = {0, 0, 1, 1, 1, 1, 1, 1, 1};
  const int64_t published_task_counts[case_count] = {0, 0, 0, 0, 0, 0, 1, 1, 1};
  const int64_t failed_task_counts[case_count] = {0, 0, 0, 0, 0, 0, 0, 0, 1};

  for (int64_t i = 0; i < case_count; ++i) {
    reserved_meta_scns.reuse();
    // 最后一轮取消 reservation，单独验证 MAX 不施加发布版本约束。
    if (reserved_scns[i].is_valid()) {
      ASSERT_EQ(OB_SUCCESS, reserved_meta_scns.push_back(reserved_scns[i]));
    }
    ASSERT_EQ(expected_rets[i], gen_task.generate_ls_ckpt_gc_task_(
        snapshot_meta_scn, safe_scns[i], published_meta_scns[i], reserved_meta_scns)) << i;
    gc_tasks.reuse();
    target_task.reset();
    allocator.reuse();
    ASSERT_EQ(OB_SUCCESS, ckpt_gc_task.get_ls_ckpt_gc_infos_(allocator, gc_tasks)) << i;
    ASSERT_EQ(OB_SUCCESS, find_ls_ckpt_gc_task(gc_tasks, old_scn, target_task, match_count));
    ASSERT_EQ(old_task_counts[i], match_count) << i;
    ASSERT_EQ(OB_SUCCESS, find_ls_ckpt_gc_task(gc_tasks, published_scn, target_task, match_count));
    ASSERT_EQ(published_task_counts[i], match_count) << i;
    ASSERT_EQ(OB_SUCCESS, find_ls_ckpt_gc_task(gc_tasks, failed_scn, target_task, match_count));
    ASSERT_EQ(failed_task_counts[i], match_count) << i;
  }
}

// 幂等：对同一 ls_id + meta_scns 再调一次 batch_del_ls_ckpt 不能报错
// —— 覆盖 §3.4 "shared_meta 已删、对象残留"（此处对象本就不存在，等价场景）。
TEST_F(TestSSCheckpointGC, batch_del_idempotent)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls_id(1600);
  ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_srv);

  ObSEArray<SCN, 4> pre;
  ASSERT_EQ(OB_SUCCESS, list_ls_ckpt_meta_scns(ls_id, pre));
  if (pre.count() > 0) {
    ASSERT_EQ(OB_SUCCESS, meta_srv->batch_del_ls_ckpt(ls_id, pre));
  }

  ObSEArray<SCN, 2> scns;
  const SCN s1 = make_scn(5000);
  const SCN s2 = make_scn(5001);
  ASSERT_EQ(OB_SUCCESS, scns.push_back(s1));
  ASSERT_EQ(OB_SUCCESS, scns.push_back(s2));
  ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_row(ls_id, s1));
  ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_row(ls_id, s2));

  // 第一次：成功删除。
  ASSERT_EQ(OB_SUCCESS, meta_srv->batch_del_ls_ckpt(ls_id, scns));
  // 第二次：行已删、对象不存在 —— 仍返回 SUCCESS（幂等）。
  ASSERT_EQ(OB_SUCCESS, meta_srv->batch_del_ls_ckpt(ls_id, scns));

  ObSEArray<SCN, 2> after;
  ASSERT_EQ(OB_SUCCESS, list_ls_ckpt_meta_scns(ls_id, after));
  ASSERT_EQ(0, after.count());
}

// CHECKPOINT ctrl 行（is_ls_type）不应生成 FAIL_GC 任务（§3 Phase 3 gen 侧保护语义保留）。
TEST_F(TestSSCheckpointGC, DISABLED_gen_need_generate_task_checkpoint_false)
{
  // 直接使用 sys 租户测试 gen 侧的纯内存分类逻辑。
  const uint64_t test_tenant_id = OB_SYS_TENANT_ID;
  volatile bool task_stopped = false;
  ObSSFailGCGenTask gen_task(task_stopped);
  ASSERT_EQ(OB_SUCCESS, gen_task.init(test_tenant_id));

  ObSSMetaCtrlKeyInfo key_info;
  key_info.reset();
  key_info.tenant_id_ = test_tenant_id;
  key_info.ls_id_ = SYS_LS;
  key_info.key_.assign_type(blocksstable::ObStorageObjectType::SHARED_LS_CKPT);
  key_info.key_.ls_key_.first_id_ = key_info.ls_id_.id();
  key_info.key_.ls_key_.second_id_ = 0;
  key_info.key_.ls_key_.control_scn_ = make_scn(100);
  ASSERT_TRUE(key_info.is_valid());

  ObSSMetaCtrlValue ctrl_value;
  ctrl_value.ctrl_info_.lease_epoch_ = 1;
  ctrl_value.ctrl_info_.op_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, ctrl_value.ctrl_info_.status_.set_status_start());
  ASSERT_EQ(OB_SUCCESS, ctrl_value.ctrl_info_.status_.set_status_abandon());
  ctrl_value.object_info_.set_none();
  ASSERT_TRUE(ctrl_value.is_valid());

  // bool need_generate_task = true;
  // ObSSFailGCTabletMap fail_gc_tablet_map;
  // ASSERT_EQ(OB_SUCCESS,
  //           fail_gc_tablet_map.create(16, ObMemAttr(test_tenant_id, "SSFailGCMap")));
  // ASSERT_EQ(OB_SUCCESS, gen_task.check_need_generate_task_(
  //     key_info, ctrl_value, fail_gc_tablet_map, need_generate_task));
  // ASSERT_FALSE(need_generate_task);
  // gen_task.destroy();
}

// 模拟 batch_del_ls_ckpt 在批内删除一部分后失败：前缀已经不存在，后缀仍在。
// 对完整批次重放必须幂等跳过已删前缀，并继续回收剩余后缀。
TEST_F(TestSSCheckpointGC, batch_delete_replays_partially_deleted_batch)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));

  volatile bool stopped = false;
  ObSSCkptGCTask task(stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));

  const ObLSID ls_id(1820);
  ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_srv);
  ASSERT_EQ(OB_SUCCESS, clear_ls_ckpt_rows(ls_id));

  ObSEArray<SCN, 8> full_batch;
  ObSEArray<SCN, 8> deleted_prefix;
  for (int64_t i = 0; i < 5; ++i) {
    const SCN meta_scn = make_scn(18200 + i);
    ASSERT_EQ(OB_SUCCESS, full_batch.push_back(meta_scn));
    ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_row(ls_id, meta_scn));
    if (i < 2) {
      ASSERT_EQ(OB_SUCCESS, deleted_prefix.push_back(meta_scn));
    }
  }

  ASSERT_EQ(OB_SUCCESS, meta_srv->batch_del_ls_ckpt(ls_id, deleted_prefix));
  ObSEArray<SCN, 8> after_partial_delete;
  ASSERT_EQ(OB_SUCCESS, list_ls_ckpt_meta_scns(ls_id, after_partial_delete));
  ASSERT_EQ(3, after_partial_delete.count());

  ASSERT_EQ(OB_SUCCESS, task.batch_del_ls_ckpts_with_retry(ls_id, full_batch));
  ObSEArray<SCN, 8> after_replay;
  ASSERT_EQ(OB_SUCCESS, list_ls_ckpt_meta_scns(ls_id, after_replay));
  ASSERT_EQ(0, after_replay.count());

  task.destroy();
}

// 底层批删接口的 LS 隔离：回收 ls_A 不应影响 ls_B 的 checkpoint 行。
TEST_F(TestSSCheckpointGC, gc_isolation_between_ls)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));

  volatile bool stopped = false;
  ObSSCkptGCTask task(stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));

  const ObLSID ls_a(1900);
  const ObLSID ls_b(1901);
  ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_srv);
  ASSERT_EQ(OB_SUCCESS, clear_ls_ckpt_rows(ls_a));
  ASSERT_EQ(OB_SUCCESS, clear_ls_ckpt_rows(ls_b));

  ObSEArray<SCN, 4> a_scns;
  ObSEArray<SCN, 4> b_scns;
  for (int64_t i = 0; i < 3; ++i) {
    const SCN a = make_scn(20000 + i);
    const SCN b = make_scn(30000 + i);
    ASSERT_EQ(OB_SUCCESS, a_scns.push_back(a));
    ASSERT_EQ(OB_SUCCESS, b_scns.push_back(b));
    ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_row(ls_a, a));
    ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_row(ls_b, b));
  }

  // 只回收 ls_a。
  ASSERT_EQ(OB_SUCCESS, meta_srv->batch_del_ls_ckpt(ls_a, a_scns));

  ObSEArray<SCN, 4> after_a;
  ObSEArray<SCN, 4> after_b;
  ASSERT_EQ(OB_SUCCESS, list_ls_ckpt_meta_scns(ls_a, after_a));
  ASSERT_EQ(OB_SUCCESS, list_ls_ckpt_meta_scns(ls_b, after_b));
  ASSERT_EQ(0, after_a.count());
  ASSERT_EQ(3, after_b.count()); // ls_b 未受影响

  ASSERT_EQ(OB_SUCCESS, clear_ls_ckpt_rows(ls_b));
  task.destroy();
}

// meta_srv->batch_del_ls_ckpt 参数校验：空 batch 与非法 ls_id 均返回 OB_INVALID_ARGUMENT。
TEST_F(TestSSCheckpointGC, batch_del_ckpts_argument_validation)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));

  volatile bool stopped = false;
  ObSSCkptGCTask task(stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_srv);

  // 空 batch。
  ObSEArray<SCN, 2> empty;
  ASSERT_EQ(OB_INVALID_ARGUMENT, meta_srv->batch_del_ls_ckpt(ObLSID(2000), empty));

  // 非法 ls_id。
  ObSEArray<SCN, 2> scns;
  ASSERT_EQ(OB_SUCCESS, scns.push_back(make_scn(1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, meta_srv->batch_del_ls_ckpt(ObLSID(), scns));

  task.destroy();
}

// batch_del_ls_ckpt 只允许 sys 租户：在 user 租户上下文调用应返回 OB_INVALID_ARGUMENT
// —— 覆盖接口 §3.2 的 sys-only 语义，保证 user 租户不会误删 shared_meta 行。
TEST_F(TestSSCheckpointGC, batch_del_rejected_on_user_tenant)
{
  ASSERT_TRUE(is_user_tenant(tenant_id_));
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));

  ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_srv);

  ObSEArray<SCN, 2> scns;
  ASSERT_EQ(OB_SUCCESS, scns.push_back(make_scn(1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, meta_srv->batch_del_ls_ckpt(ObLSID(1001), scns));
}

// batch_del_ls_ckpt 内部参数校验：非法 meta_scn（min_scn 之外的空 SCN）应报错。
// batch 内含非法 SCN → OB_INVALID_ARGUMENT（sys 上下文，绕过 sys-only 前置判断）。
TEST_F(TestSSCheckpointGC, batch_del_invalid_scn)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_srv);

  ObSEArray<SCN, 2> scns;
  ASSERT_EQ(OB_SUCCESS, scns.push_back(SCN())); // invalid scn
  ASSERT_EQ(OB_INVALID_ARGUMENT, meta_srv->batch_del_ls_ckpt(ObLSID(2100), scns));
}

// 真实对象双删（评审 P1-5）：伪造 inner table 行 + 真实对象存储对象，
// 走 meta_srv->batch_del_ls_ckpt -> batch_del_ls_ckpt，断言【行 + 对象】都被删除。
// 本用例会真正覆盖 batch_del_ls_ckpt 的对象存储删除分支。
TEST_F(TestSSCheckpointGC, batch_del_removes_real_object)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));

  volatile bool stopped = false;
  ObSSCkptGCTask task(stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));

  const ObLSID ls_id(2300);
  ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_srv);
  ASSERT_EQ(OB_SUCCESS, clear_ls_ckpt_rows(ls_id));

  const int64_t kRows = 1;
  ObSEArray<SCN, kRows> scns;
  for (int64_t i = 0; i < kRows; ++i) {
    const SCN scn = make_scn(40000 + i);
    ASSERT_EQ(OB_SUCCESS, scns.push_back(scn));
    ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_with_object(ls_id, scn));
    // 前置断言：对象确实已写入。
    assert_object_exists_(ls_ckpt_oss_macro_id(ls_id, scn), true);
  }
  ObSEArray<SCN, kRows> before;
  ASSERT_EQ(OB_SUCCESS, list_ls_ckpt_meta_scns(ls_id, before));
  ASSERT_EQ(kRows, before.count());

  // 双删。
  ASSERT_EQ(OB_SUCCESS, meta_srv->batch_del_ls_ckpt(ls_id, scns));

  // 行删净。
  ObSEArray<SCN, kRows> after;
  ASSERT_EQ(OB_SUCCESS, list_ls_ckpt_meta_scns(ls_id, after));
  ASSERT_EQ(0, after.count());
  // 对象也删净。
  for (int64_t i = 0; i < kRows; ++i) {
    assert_object_exists_(ls_ckpt_oss_macro_id(ls_id, scns.at(i)), false);
  }

  task.destroy();
}

// 幂等补删「行已删、对象残留」（评审 P1-5 补充 + §3.4）：
// 先只删行、手动重建对象模拟"上一轮删行成功、删对象失败留下残留"，
// 再次调 batch_del_ls_ckpt 必须把残留对象补删掉且返回成功 —— 不依赖 MarkSweep 兜底。
TEST_F(TestSSCheckpointGC, batch_del_reclaims_orphan_object)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls_id(2400);
  ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_srv);
  ASSERT_EQ(OB_SUCCESS, clear_ls_ckpt_rows(ls_id));

  const SCN scn = make_scn(50000);
  ObSEArray<SCN, 1> scns;
  ASSERT_EQ(OB_SUCCESS, scns.push_back(scn));

  // 第一轮：完整双删（行 + 对象）。
  ASSERT_EQ(OB_SUCCESS, insert_forged_ls_ckpt_with_object(ls_id, scn));
  ASSERT_EQ(OB_SUCCESS, meta_srv->batch_del_ls_ckpt(ls_id, scns));
  ObSEArray<SCN, 1> mid;
  ASSERT_EQ(OB_SUCCESS, list_ls_ckpt_meta_scns(ls_id, mid));
  ASSERT_EQ(0, mid.count());
  assert_object_exists_(ls_ckpt_oss_macro_id(ls_id, scn), false);

  // 模拟"行已删、对象残留"：手动重建对象（不重建行）。
  ASSERT_EQ(OB_SUCCESS, write_object_(ls_ckpt_oss_macro_id(ls_id, scn)));
  assert_object_exists_(ls_ckpt_oss_macro_id(ls_id, scn), true);

  // 第二轮：行已不存在，接口仍需按 meta_scn 补删对象且返回成功（幂等）。
  ASSERT_EQ(OB_SUCCESS, meta_srv->batch_del_ls_ckpt(ls_id, scns));
  assert_object_exists_(ls_ckpt_oss_macro_id(ls_id, scn), false);
}

// checkpoint 对象定位位点、LS 数据位点和 Tablet 精确引用位点分属不同域，不能混用；
// merge 必须采用 checkpoint key 携带的 meta_scn 覆盖 study 结果。
TEST_F(TestSSCheckpointGC, ckpt_scn_domains_separated_and_merge_uses_ckpt_meta_scn)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));

  const ObTabletID tablet_id(ObTabletID::MIN_USER_TABLET_ID + 100);
  const SCN transfer_scn = make_scn(60200);
  const SCN ckpt_tablet_meta_scn = make_scn(60300);
  ObSSMetaTabletKey tablet_key;
  tablet_key.ls_id_ = SYS_LS;
  tablet_key.tablet_id_ = tablet_id;
  tablet_key.reorganization_scn_ = transfer_scn;
  tablet_key.meta_scn_ = ckpt_tablet_meta_scn;
  ASSERT_TRUE(tablet_key.is_valid());
  const blocksstable::MacroBlockId tablet_meta_key =
      tablet_key.convert_to_macro_id(false/*is_in_table*/);
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_TABLET_META,
            tablet_meta_key.storage_object_type());
  ASSERT_EQ(static_cast<int64_t>(transfer_scn.get_val_for_inner_table_field()),
            tablet_meta_key.reorganization_scn());
  ASSERT_EQ(static_cast<int64_t>(ckpt_tablet_meta_scn.get_val_for_inner_table_field()),
            tablet_meta_key.meta_scn());

  // inner tablet 的 third_id 表示 LS ID，不能按 reorganization_scn 解释。
  ObSSMetaTabletKey inner_tablet_key;
  inner_tablet_key.ls_id_ = SYS_LS;
  inner_tablet_key.tablet_id_ = ObTabletID(ObTabletID::LS_TX_DATA_TABLET_ID);
  inner_tablet_key.reorganization_scn_ = SCN::min_scn();
  inner_tablet_key.meta_scn_ = make_scn(60400);
  const blocksstable::MacroBlockId inner_tablet_meta_key =
      inner_tablet_key.convert_to_macro_id(false/*is_in_table*/);
  ASSERT_TRUE(inner_tablet_meta_key.meta_is_inner_tablet());
  ASSERT_EQ(SYS_LS.id(), inner_tablet_meta_key.ls_id());
  ASSERT_EQ(static_cast<int64_t>(
                inner_tablet_key.meta_scn_.get_val_for_inner_table_field()),
            inner_tablet_meta_key.meta_scn());

  ObSSGCTabletWithMetaScnArray tablet_with_meta_scn_arr;
  ASSERT_EQ(OB_SUCCESS, tablet_with_meta_scn_arr.push_back(
      ObSSGCTabletWithMetaScn(
          SYS_LS, tablet_id, transfer_scn, make_scn(61000))));

  volatile bool stopped = false;
  ObSSMarkSweepGCGenTask task(stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));

  // checkpoint 读取到的 tablet key 直接喂给 merge：study 里更新的 61000 必须被
  // checkpoint 引用的精确位点 60300 覆盖，避免 GC 越过 checkpoint 仍引用的版本。
  ObSEArray<blocksstable::MacroBlockId, 1> loaded_tablet_meta_keys;
  ASSERT_EQ(OB_SUCCESS, loaded_tablet_meta_keys.push_back(tablet_meta_key));
  ASSERT_EQ(OB_SUCCESS, task.merge_tablet_meta_scns_from_ckpt_(
      make_scn(62000), loaded_tablet_meta_keys, tablet_with_meta_scn_arr));
  ASSERT_EQ(1, tablet_with_meta_scn_arr.count());
  ASSERT_EQ(ckpt_tablet_meta_scn, tablet_with_meta_scn_arr.at(0).meta_scn_);
  task.destroy();
}

// checkpoint Tablet 晚于本轮快照时，study 中缺失是合法的，本轮直接跳过。
TEST_F(TestSSCheckpointGC, merge_ckpt_skips_tablet_newer_than_snapshot)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));

  const SCN snapshot_meta_scn = make_scn(65000);
  ObSSMetaTabletKey tablet_key;
  tablet_key.ls_id_ = SYS_LS;
  tablet_key.tablet_id_ = ObTabletID(ObTabletID::MIN_USER_TABLET_ID + 201);
  tablet_key.reorganization_scn_ = make_scn(64900);
  tablet_key.meta_scn_ = make_scn(65001);
  ASSERT_TRUE(tablet_key.is_valid());

  ObSEArray<blocksstable::MacroBlockId, 1> tablet_meta_keys;
  ASSERT_EQ(OB_SUCCESS, tablet_meta_keys.push_back(
      tablet_key.convert_to_macro_id(false/*is_in_table*/)));
  ObSSGCTabletWithMetaScnArray study_tablets;
  volatile bool stopped = false;
  TestableSSMarkSweepGCGenTask task(stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  EXPECT_EQ(OB_SUCCESS, task.merge_tablet_meta_scns_from_ckpt_(
      snapshot_meta_scn, tablet_meta_keys, study_tablets));
  EXPECT_TRUE(study_tablets.empty());
  task.destroy();
}

// checkpoint Tablet 已进入本轮快照却不在 study 中时，必须终止本轮并等待重试。
TEST_F(TestSSCheckpointGC, merge_ckpt_missing_visible_tablet_returns_eagain)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));

  const SCN snapshot_meta_scn = make_scn(66000);
  ObSSMetaTabletKey tablet_key;
  tablet_key.ls_id_ = SYS_LS;
  tablet_key.tablet_id_ = ObTabletID(ObTabletID::MIN_USER_TABLET_ID + 202);
  tablet_key.reorganization_scn_ = make_scn(65900);
  tablet_key.meta_scn_ = snapshot_meta_scn;
  ASSERT_TRUE(tablet_key.is_valid());

  ObSEArray<blocksstable::MacroBlockId, 1> tablet_meta_keys;
  ASSERT_EQ(OB_SUCCESS, tablet_meta_keys.push_back(
      tablet_key.convert_to_macro_id(false/*is_in_table*/)));
  ObSSGCTabletWithMetaScnArray study_tablets;
  volatile bool stopped = false;
  TestableSSMarkSweepGCGenTask task(stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  EXPECT_EQ(OB_EAGAIN, task.merge_tablet_meta_scns_from_ckpt_(
      snapshot_meta_scn, tablet_meta_keys, study_tablets));
  EXPECT_TRUE(study_tablets.empty());
  task.destroy();
}

// 位点读取失败时不能带着半成品继续：必须清空出参，且不去读任何 checkpoint 对象。
// 本 env 未开 logservice，get_ls_ckpt_meta_scn 固定返回 OB_NOT_SUPPORTED，
// 正好用来驱动"首个 get 即失败"这条分支；未发布与重试路径见文件头覆盖边界。
TEST_F(TestSSCheckpointGC, read_ckpt_fails_fast_and_clears_output)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));

  volatile bool stopped = false;
  TestableSSMarkSweepGCGenTask task(stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));

  SCN ckpt_meta_scn = make_scn(64100);
  SCN ss_checkpoint_scn = make_scn(64000);
  bool has_ls_ckpt = true;
  ObSEArray<blocksstable::MacroBlockId, 1> tablet_meta_keys;
  ASSERT_EQ(OB_SUCCESS, tablet_meta_keys.push_back(blocksstable::MacroBlockId()));
  ASSERT_EQ(OB_NOT_SUPPORTED,
      task.read_ls_ckpt_with_retry_(
          ss_checkpoint_scn, tablet_meta_keys, ckpt_meta_scn, has_ls_ckpt));
  ASSERT_EQ(0, task.read_count());
  ASSERT_FALSE(has_ls_ckpt);
  ASSERT_FALSE(ckpt_meta_scn.is_valid());
  ASSERT_FALSE(ss_checkpoint_scn.is_valid());
  ASSERT_TRUE(tablet_meta_keys.empty());
  task.destroy();
}

TEST_F(TestSSCheckpointGC, read_ckpt_retries_with_new_generation)
{
  share::ObTenantSwitchGuard guard;
  volatile bool stopped = false;
  MockSSMarkSweepGCGenTask task(stopped);
  const SCN old_ckpt_meta_scn = make_scn(64100);
  const SCN new_ckpt_meta_scn = make_scn(65100);
  const SCN old_ss_checkpoint_scn = make_scn(63100);
  const SCN new_ss_checkpoint_scn = make_scn(63500);
  ObSSMetaTabletKey old_tablet_key;
  ObSSMetaTabletKey new_tablet_key;
  SCN ckpt_meta_scn = make_scn(66100);
  SCN ss_checkpoint_scn = make_scn(66000);
  bool has_ls_ckpt = true;
  ObSEArray<blocksstable::MacroBlockId, 1> tablet_meta_keys;
  ::testing::InSequence sequence;
  MockSSMarkSweepGCGenTask *previous_active_mock = MockSSMarkSweepGCGenTask::active_mock_;

  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  DEFER(MockSSMarkSweepGCGenTask::active_mock_ = previous_active_mock);
  MockSSMarkSweepGCGenTask::active_mock_ = &task;
  old_tablet_key.ls_id_ = SYS_LS;
  old_tablet_key.tablet_id_ = ObTabletID(ObTabletID::MIN_USER_TABLET_ID + 110);
  old_tablet_key.reorganization_scn_ = make_scn(62000);
  old_tablet_key.meta_scn_ = make_scn(63200);
  new_tablet_key.ls_id_ = SYS_LS;
  new_tablet_key.tablet_id_ = ObTabletID(ObTabletID::MIN_USER_TABLET_ID + 111);
  new_tablet_key.reorganization_scn_ = make_scn(62100);
  new_tablet_key.meta_scn_ = make_scn(63600);
  ASSERT_TRUE(old_tablet_key.is_valid());
  ASSERT_TRUE(new_tablet_key.is_valid());
  task.success_ckpt_meta_scn_ = new_ckpt_meta_scn;
  task.missing_ss_checkpoint_scn_ = old_ss_checkpoint_scn;
  task.success_ss_checkpoint_scn_ = new_ss_checkpoint_scn;
  task.missing_tablet_meta_key_ = old_tablet_key.convert_to_macro_id(false/*is_in_table*/);
  task.success_tablet_meta_key_ = new_tablet_key.convert_to_macro_id(false/*is_in_table*/);
  new_tablet_key.tablet_id_ = ObTabletID(ObTabletID::MIN_USER_TABLET_ID + 113);
  new_tablet_key.meta_scn_ = make_scn(63700);
  ASSERT_TRUE(new_tablet_key.is_valid());
  task.second_success_tablet_meta_key_ = new_tablet_key.convert_to_macro_id(false/*is_in_table*/);
  ASSERT_EQ(OB_SUCCESS, tablet_meta_keys.push_back(task.missing_tablet_meta_key_));

  // 调用顺序同时证明对象缺失后重新读取 PALF，没有复用旧代位点。
  EXPECT_CALL(task, get_ls_ckpt_meta_scn_(::testing::_))
      .Times(1)
      .WillOnce(::testing::DoAll(
          ::testing::SetArgReferee<0>(old_ckpt_meta_scn), ::testing::Return(OB_SUCCESS)));
  EXPECT_CALL(task, read_ls_ckpt_data_(old_ckpt_meta_scn, ::testing::_, ::testing::_))
      .Times(1)
      .WillOnce(::testing::Invoke(&task, &MockSSMarkSweepGCGenTask::fill_ls_ckpt_data));
  EXPECT_CALL(task, get_ls_ckpt_meta_scn_(::testing::_))
      .Times(1)
      .WillOnce(::testing::DoAll(
          ::testing::SetArgReferee<0>(new_ckpt_meta_scn), ::testing::Return(OB_SUCCESS)));
  EXPECT_CALL(task, read_ls_ckpt_data_(new_ckpt_meta_scn, ::testing::_, ::testing::_))
      .Times(1)
      .WillOnce(::testing::Invoke(&task, &MockSSMarkSweepGCGenTask::fill_ls_ckpt_data));

  ASSERT_EQ(OB_SUCCESS, task.read_ls_ckpt_with_retry_(
      ss_checkpoint_scn, tablet_meta_keys, ckpt_meta_scn, has_ls_ckpt));
  ASSERT_TRUE(has_ls_ckpt);
  ASSERT_EQ(new_ckpt_meta_scn, ckpt_meta_scn);
  ASSERT_EQ(new_ss_checkpoint_scn, ss_checkpoint_scn);
  ASSERT_EQ(2, tablet_meta_keys.count());
  ASSERT_EQ(task.success_tablet_meta_key_, tablet_meta_keys.at(0));
  ASSERT_EQ(task.second_success_tablet_meta_key_, tablet_meta_keys.at(1));
}

TEST_F(TestSSCheckpointGC, read_ckpt_retry_exhaustion_clears_output)
{
  share::ObTenantSwitchGuard guard;
  volatile bool stopped = false;
  MockSSMarkSweepGCGenTask task(stopped);
  const int64_t EXPECTED_READ_COUNT = 4;
  const SCN published_ckpt_meta_scn = make_scn(64100);
  ObSSMetaTabletKey tablet_key;
  SCN ckpt_meta_scn = make_scn(66100);
  SCN ss_checkpoint_scn = make_scn(66000);
  bool has_ls_ckpt = true;
  ObSEArray<blocksstable::MacroBlockId, 1> tablet_meta_keys;
  ::testing::InSequence sequence;
  MockSSMarkSweepGCGenTask *previous_active_mock = MockSSMarkSweepGCGenTask::active_mock_;

  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  DEFER(MockSSMarkSweepGCGenTask::active_mock_ = previous_active_mock);
  MockSSMarkSweepGCGenTask::active_mock_ = &task;
  tablet_key.ls_id_ = SYS_LS;
  tablet_key.tablet_id_ = ObTabletID(ObTabletID::MIN_USER_TABLET_ID + 112);
  tablet_key.reorganization_scn_ = make_scn(62000);
  tablet_key.meta_scn_ = make_scn(63200);
  ASSERT_TRUE(tablet_key.is_valid());
  task.missing_ss_checkpoint_scn_ = make_scn(63100);
  task.missing_tablet_meta_key_ = tablet_key.convert_to_macro_id(false/*is_in_table*/);
  ASSERT_EQ(OB_SUCCESS, tablet_meta_keys.push_back(task.missing_tablet_meta_key_));

  // 初次读取加三次重试；每次均填入脏输出并返回对象缺失，不能降级成未发布成功。
  for (int64_t i = 0; i < EXPECTED_READ_COUNT; ++i) {
    EXPECT_CALL(task, get_ls_ckpt_meta_scn_(::testing::_))
        .Times(1)
        .WillOnce(::testing::DoAll(
            ::testing::SetArgReferee<0>(published_ckpt_meta_scn), ::testing::Return(OB_SUCCESS)));
    EXPECT_CALL(task, read_ls_ckpt_data_(published_ckpt_meta_scn, ::testing::_, ::testing::_))
        .Times(1)
        .WillOnce(::testing::Invoke(&task, &MockSSMarkSweepGCGenTask::fill_ls_ckpt_data));
  }

  ASSERT_EQ(OB_EAGAIN, task.read_ls_ckpt_with_retry_(
      ss_checkpoint_scn, tablet_meta_keys, ckpt_meta_scn, has_ls_ckpt));
  ASSERT_FALSE(has_ls_ckpt);
  ASSERT_FALSE(ckpt_meta_scn.is_valid());
  ASSERT_FALSE(ss_checkpoint_scn.is_valid());
  ASSERT_TRUE(tablet_meta_keys.empty());
}

// deleted-tablet filter 与 DAG 必须消费同一份有效 LS 数据位点，不能再次读取 live LS_META。
// 判定完全由传入位点驱动：位点已推进到 deleted_scn 时保留 study 结果；
// 位点滞后时将 meta_scn 置为 min，交由 Sort 复用已持久化 kept_meta_scn。
TEST_F(TestSSCheckpointGC, filter_uses_effective_ls_checkpoint_scn_map)
{
  volatile bool stopped = false;
  ObSSMarkSweepGCGenTask task(stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));

  const ObTabletID tablet_id(ObTabletID::MIN_USER_TABLET_ID + 101);
  const SCN transfer_scn = make_scn(65100);
  const SCN deleted_scn = make_scn(66000);
  const SCN tablet_meta_scn = make_scn(67000);
  const SCN snapshot_meta_scn = make_scn(68000);
  ObSSGCTabletWithDelScnArray gc_tablets;
  ASSERT_EQ(OB_SUCCESS, gc_tablets.push_back(ObSSGCTabletWithDelScn(
      SYS_LS, tablet_id, transfer_scn, deleted_scn)));
  ObSSGCTabletWithMetaScnArray tablet_with_meta_scn_arr;
  ASSERT_EQ(OB_SUCCESS, tablet_with_meta_scn_arr.push_back(ObSSGCTabletWithMetaScn(
      SYS_LS, tablet_id, transfer_scn, tablet_meta_scn)));

  // 传入位点已推进到 deleted_scn：放行，且 filter 不改写 study 得到的 meta_scn。
  ASSERT_EQ(OB_SUCCESS, task.filter_by_ls_ss_checkpoint_scn_(
      SYS_LS, snapshot_meta_scn, deleted_scn,
      gc_tablets, tablet_with_meta_scn_arr));
  ASSERT_EQ(tablet_meta_scn, tablet_with_meta_scn_arr.at(0).meta_scn_);

  // 传入位点滞后于 deleted_scn：写入 min 哨兵，交由 Sort 使用持久化保留位点。
  ASSERT_EQ(OB_SUCCESS, task.filter_by_ls_ss_checkpoint_scn_(
      SYS_LS, snapshot_meta_scn, make_scn(65000),
      gc_tablets, tablet_with_meta_scn_arr));
  ASSERT_EQ(SCN::min_scn(), tablet_with_meta_scn_arr.at(0).meta_scn_);
  task.destroy();
}

// runner 仅 sys 租户启动 ckpt gc（评审 P0-2）：start_ckpt_gc_task_ 对 sys 启动、对 user 跳过。
// 直接构造 ObSSGCTaskRunner，验证两租户下 start_ckpt_gc_task_() 的分支。
TEST_F(TestSSCheckpointGC, runner_starts_ckpt_gc_only_on_sys)
{
  // sys 租户：init + start_ckpt_gc_task_ 应成功（真正 schedule timer）。
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
    ObSSGCTaskRunner runner;
    ASSERT_EQ(OB_SUCCESS, runner.init(OB_SYS_TENANT_ID));
    ASSERT_TRUE(is_sys_tenant(runner.tenant_id_));
    // mid_tg_id_ 由 init 分配（有效）；start_ckpt_gc_task_ 内 is_sys_tenant 为真会真正 start。
    ASSERT_GT(runner.mid_tg_id_, 0);
    ASSERT_EQ(OB_SUCCESS, TG_START(runner.mid_tg_id_));
    ASSERT_EQ(OB_SUCCESS, runner.start_ckpt_gc_task_());
    runner.destroy();
  }
  // user 租户：start_ckpt_gc_task_ 应直接跳过（不 start timer），返回成功。
  {
    ASSERT_TRUE(is_user_tenant(tenant_id_));
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    ObSSGCTaskRunner runner;
    ASSERT_EQ(OB_SUCCESS, runner.init(tenant_id_));
    ASSERT_FALSE(is_sys_tenant(runner.tenant_id_));
    // 未 TG_START，start_ckpt_gc_task_ 走 else 分支跳过，不会因 timer 未启动而报错。
    ASSERT_EQ(OB_SUCCESS, runner.start_ckpt_gc_task_());
    runner.destroy();
  }
}

// start() 只校验自身状态，不做租户判断：sys-only 门控由 ObSSGCTaskRunner::start_ckpt_gc_task_
// 承担（见 runner_starts_ckpt_gc_only_on_sys）。这里锁定 start() 的状态校验语义。
TEST_F(TestSSCheckpointGC, ckpt_gc_task_start_validates_state)
{
  volatile bool stopped = false;

  // 未初始化检查必须优先于 schedule，不能拿未初始化任务去注册定时任务。
  ObSSCkptGCTask not_inited_task(stopped);
  ASSERT_EQ(OB_NOT_INIT, not_inited_task.start(-1));

  ASSERT_TRUE(is_user_tenant(tenant_id_));
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ObSSCkptGCTask user_task(stopped);
  ASSERT_EQ(OB_SUCCESS, user_task.init(tenant_id_));
  ASSERT_FALSE(is_sys_tenant(user_task.tenant_id_));
  // 已初始化后 start() 会直接进入 schedule，无效 tg_id 必须被拒绝而不是静默跳过。
  ASSERT_EQ(OB_INVALID_ARGUMENT, user_task.start(-1));
  user_task.destroy();
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int64_t c = 0;
  char *log_level = (char *)"INFO";
  char buf[1000];
  const int64_t cur_time_ns = oceanbase::common::ObTimeUtility::current_time_ns();
  memset(buf, 0, sizeof(buf));
  oceanbase::common::databuff_printf(
      buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s"
      "&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns,
      oceanbase::unittest::S3_ENDPOINT, oceanbase::unittest::S3_AK,
      oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  while (EOF != (c = getopt(argc, argv, "l:"))) {
    switch (c) {
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
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
