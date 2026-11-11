/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#define UNITTEST

#include <gmock/gmock.h>
#include <cstdlib>
#include "lib/utility/ob_defer.h"
#define protected public
#define private public
#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/restore/ob_object_storage_base.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "share/io/ob_io_define.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/compaction_v2/ob_ss_compact_helper.h"
#include "storage/ss_garbage_collector/dag/ob_ss_fail_gc_dag.h"
#include "storage/ss_garbage_collector/ob_ss_fail_gc_util.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_sstable_writer.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_dag.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_macro_block_writer.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/garbage_collector/ob_mark_sweep_pipeline.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/incremental/atomic_protocol/ob_tablet_atomic_protocol.h"
#include "storage/incremental/ssmeta/ob_ss_meta_atomic_op.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_iterator.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_util.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "share/vector_index/quantization/ob_quantization_encoder.h"
#include "storage/compaction_v2/ob_ss_major_merge_ctx.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "src/storage/ss_garbage_collector/dag/ob_ss_gc_exec_dag.h"
#include "src/storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#include "src/storage/shared_storage/ob_remote_io_adapter.h"
#include "src/storage/shared_storage/ob_ss_object_access_util.h"
#include "src/storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"

namespace oceanbase
{
// Override the weak symbol in ob_simple_server.cpp so the simple server
// starts in shared-storage mode with a MinIO-backed object store.
char *shared_storage_info = nullptr;

// Mock implementations required by the shared-storage bootstrap path.
// All shared-storage simple_server tests need these to avoid OB_NOT_INIT
// (-4006) failures during prepare_bootstrap, where ObServerZoneOpService
// and ObMasterKeyUtil call into PALF KV (not available in test environments).
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
using namespace unittest;
namespace storage
{
using namespace common;
using namespace share;

// IVF Fail GC 协议、对象枚举和轮次隔离回归。
class TestIVFFailGC : public ObSimpleClusterTestBase
{
public:
  TestIVFFailGC()
    : ObSimpleClusterTestBase("test_ivf_fail_gc", "50G", "50G", "50G")
  {
    if (!is_started_) {
      // 本套件不使用系统存储过程包，启动与建租户无需等待其加载完成。
      get_curr_simple_server().set_extra_optstr("_enable_async_load_sys_package=1");
    }
  }

  static uint64_t tenant_id_;

  // 各用例独立计时，避免前序用例的耗时影响当前用例。
  static constexpr int64_t FAST_CASE_TIMEOUT_US = 60L * 1000 * 1000;
  static constexpr int64_t FAST_SQL_TIMEOUT_US = 3L * 1000 * 1000;

  class FastCaseBudget
  {
  public:
    FastCaseBudget()
      : saved_timeout_(THIS_WORKER.get_timeout_ts()),
        start_us_(ObTimeUtility::current_time())
    {
      THIS_WORKER.set_timeout_ts(deadline_us());
    }
    ~FastCaseBudget() { THIS_WORKER.set_timeout_ts(saved_timeout_); }
    int64_t deadline_us() const { return start_us_ + FAST_CASE_TIMEOUT_US; }
    int64_t elapsed_us() const { return ObTimeUtility::current_time() - start_us_; }
  private:
    int64_t saved_timeout_;
    int64_t start_us_;
  };

  void ensure_tablet_lock_(const ObTabletID &tablet_id)
  {
    using namespace blocksstable;
    ObSSMetaCtrlKey meta_key(ObStorageObjectType::SHARED_TABLET_META,
        tablet_id, SCN::min_scn(), SCN::min_scn(), ObLSID(ObLSID::SYS_LS_ID));
    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, meta_key.get_lock_row_key(lock_key));
    ObLockAtomicProtocol protocol;
    ASSERT_EQ(OB_SUCCESS, protocol.init(tenant_id_, ObLSID(ObLSID::SYS_LS_ID), lock_key));
    ASSERT_EQ(OB_SUCCESS, protocol.check_and_create_lock_row(sql_proxy()));
  }

  int read_ctrl_(const ObSSMetaCtrlKeyInfo &key, ObSSMetaCtrlValue &value)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator(ObMemAttr(tenant_id_, "IVFFailGCTest"));
    ObString serialized_key;
    ObString serialized_value;
    ObSSMetaCtrlTableOperator op;
    ObSSTableAtomicProtocol serializer;
    int64_t pos = 0;
    if (OB_FAIL(serializer.get_row_meta_key_(key.key_, allocator, serialized_key))) {
    } else if (OB_FAIL(op.init(sql_proxy(), FAST_SQL_TIMEOUT_US))) {
    } else if (OB_FAIL(op.read_row(tenant_id_, key.ls_id_, key.key_.type_,
        serialized_key, allocator, serialized_value))) {
    } else if (OB_FAIL(value.deserialize(serialized_value.ptr(), serialized_value.length(), pos))) {
    }
    return ret;
  }

  // 跳过后台定时器和超时窗口，保留真实 COMPLETE->ABANDON、控制行删除和任务插入事务。
  void generate_orphan_gc_(const ObSSMetaCtrlKeyInfo &key,
      const ObSSMetaCtrlValue &expected, ObSharedGCTask &generated, ObIAllocator &allocator)
  {
    ObSSMetaCtrlValue value;
    ASSERT_EQ(OB_SUCCESS, read_ctrl_(key, value));
    ASSERT_TRUE(value == expected);
    ObArenaAllocator dag_allocator;
    ObSSFailGCDag dag;
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
    ObSSFailGCDagParam param;
    ASSERT_EQ(OB_SUCCESS, param.init(tenant_id_));
    ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
    ObSSFailGCDagCtx *ctx = dag.get_ctx();
    ASSERT_NE(nullptr, ctx);
    ctx->need_gc_ = true;
    ctx->sql_timeout_us_ = FAST_SQL_TIMEOUT_US;
    ctx->gc_scn_ = SCN::scn_inc(key.key_.sstable_key_.control_scn_);
    ObSSFailGCLSCtx *ls_ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(key.ls_id_, ls_ctx));
    ObSSFailGCCtrlRow row;
    ASSERT_EQ(OB_SUCCESS, row.ctrl_key_.assign(key.key_));
    row.ctrl_value_ = value;
    ObSEArray<ObSSFailGCCtrlRow, 1> rows;
    ASSERT_EQ(OB_SUCCESS, rows.push_back(row));
    ObSSFailGCExecutorTask *executor = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor));
    ASSERT_EQ(OB_SUCCESS, executor->init(ls_ctx, rows));
    // 协议在测试线程创建，无对应活跃 DAG。若 owner 判定失败，后续 count 断言立即失败。
    ObSEArray<ObSSFailGCInfo, 1> abandoned;
    ASSERT_EQ(OB_SUCCESS, executor->process_ctrl_row_(*ctx, row, abandoned));
    ASSERT_EQ(1, abandoned.count());
    ASSERT_TRUE(abandoned.at(0).need_generate_task_);
    ASSERT_EQ(OB_SUCCESS, executor->generate_fail_gc_task_(*ctx, abandoned));
    ASSERT_EQ(1, ctx->get_generated_task_count());
    ASSERT_EQ(OB_ITER_END, read_ctrl_(key, value));

    ObSharedGCTaskTableRangeIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id_, ObSharedGCTaskType::FAIL_GC, 0,
        ObSSGCTaskIdRange(0, INT64_MAX), sql_proxy()));
    bool found = false;
    ObSharedGCTask task;
    int iter_ret = OB_SUCCESS;
    while (OB_SUCCESS == (iter_ret = iter.get_next(allocator, task))) {
      ObSharedGCFailInfo info;
      ASSERT_EQ(OB_SUCCESS, info.from_value(task.value_));
      if (info.tablet_id_ == key.key_.sstable_key_.tablet_id_
          && info.op_id_ == expected.ctrl_info_.op_id_) {
        ASSERT_FALSE(found);
        ASSERT_TRUE(info.ctrl_object_info_ == expected.object_info_);
        generated = task; // 字符串已由 iterator 深拷贝到调用方 allocator。
        found = true;
      }
    }
    ASSERT_EQ(OB_ITER_END, iter_ret);
    ASSERT_TRUE(found);
  }

  void execute_gc_task_(const ObSharedGCTask &row)
  {
    ObArenaAllocator allocator;
    ObSSGCExecDag dag;
    ObSSGCExecDagInitParam param;
    param.tenant_id_ = tenant_id_;
    param.gc_type_ = row.key_.type_;
    param.allow_gc_round_id_ = row.key_.round_id_;
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
    ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
    const ObSSGCTaskIdRange range(row.key_.task_id_, row.key_.task_id_);
    if (ObSharedGCTaskType::FAIL_GC == row.key_.type_) {
      ObFailGCExecuteTask *task = nullptr;
      ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
      ASSERT_EQ(OB_SUCCESS, task->init(tenant_id_, range, range));
      ASSERT_EQ(OB_SUCCESS, task->process());
    } else {
      ObMarkSweepExecuteTask *task = nullptr;
      ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
      ASSERT_EQ(OB_SUCCESS, task->init(tenant_id_, row.key_.round_id_, range, range,
          row.key_.round_id_));
      ASSERT_EQ(OB_SUCCESS, task->process());
    }
    ObSharedGCTask history;
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
        row.key_, allocator, sql_proxy(), history));
    EXPECT_TRUE(history.value_ == row.value_);
  }

  ObISQLClient &sql_proxy()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  uint64_t meta_tid() const { return gen_meta_tenant_id(tenant_id_); }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

  static int build_sstable_macro_id_(
      const blocksstable::ObStorageObjectType obj_type,
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t fourth_id,
      const int64_t data_seq,
      const int64_t cg_id,
      const share::SCN &reorganization_scn,
      blocksstable::MacroBlockId &macro_id)
  {
    blocksstable::ObStorageObjectOpt opt;
    opt.set_ss_share_object_opt(obj_type,
                                tablet_id.is_ls_inner_tablet(),
                                ls_id.id(),
                                tablet_id.id(),
                                fourth_id,
                                data_seq,
                                cg_id,
                                reorganization_scn.get_val_for_tx());
    return blocksstable::ObObjectManager::ss_get_object_id(opt, macro_id);
  }

  static void write_macro_block_(const blocksstable::MacroBlockId &macro_id, const char payload = 'a')
  {
    static char write_buf[1024 * 1024];
    memset(write_buf, payload, sizeof(write_buf));
    blocksstable::ObStorageObjectWriteInfo write_info;
    write_info.io_desc_.set_wait_event(1);
    write_info.buffer_ = write_buf;
    write_info.offset_ = 0;
    write_info.size_ = sizeof(write_buf);
    write_info.io_timeout_ms_ = 3000;
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.write_strategy_ = blocksstable::ObStorageObjectWriteStrategy::WRITE_THROUGH;

    blocksstable::ObStorageObjectHandle write_object_handle;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
    ASSERT_EQ(OB_SUCCESS,
              ObSSObjectAccessUtil::async_write_file(write_info, write_object_handle));
    ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  }

  static void assert_block_exists_(const blocksstable::MacroBlockId &macro_id,
                                   const bool expected_exist)
  {
    bool actual = false;
    ASSERT_EQ(OB_SUCCESS,
              ObRemoteIoAdapter::is_exist(
                  MTL_ID(), macro_id, GCONF.observer_id, actual));
    char id_buf[256] = {0};
    macro_id.to_string(id_buf, sizeof(id_buf));
    ASSERT_EQ(expected_exist, actual)
        << "macro_id=" << id_buf
        << " expected_exist=" << expected_exist
        << " actual=" << actual;
  }
};

uint64_t TestIVFFailGC::tenant_id_ = OB_INVALID_TENANT_ID;

TEST_F(TestIVFFailGC, observer_start)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ObLSID(ObLSID::SYS_LS_ID), handle,
                                        ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  bool elected = false;
  for (int i = 0; i < 200; ++i) {
    ObRole role;
    int64_t proposal_id = 0;
    ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
    if (ObRole::LEADER == role) {
      elected = true;
      break;
    }
    ob_usleep(10 * 1000);
  }
  ASSERT_TRUE(elected) << "sys LS did not elect a leader within 2 s";
  LOG_INFO("observer ready, sys LS is leader");
}

TEST_F(TestIVFFailGC, setup_tenant)
{
  int create_ret = OB_SUCCESS;
  int retry_cnt = 0;
  do {
    create_ret = create_tenant("tt1");
    if (OB_SUCCESS != create_ret) {
      LOG_WARN_RET(create_ret, "create_tenant failed, will retry", K(retry_cnt));
      ob_usleep(15 * 1000 * 1000);
    }
  } while (OB_SUCCESS != create_ret && ++retry_cnt < 10);
  ASSERT_EQ(OB_SUCCESS, create_ret);
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
  ASSERT_TRUE(is_user_tenant(tenant_id_));
  LOG_INFO("user tenant created", K_(tenant_id));

  // Wait for the user-tenant SYS_LS to elect a leader so the first DAG-driven
  // SQL UPDATE on __all_shared_gc_task does not hit OB_NOT_MASTER (-4016) or
  // OB_TIMEOUT (-4012) while the LS log handler is still warming up.
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    ObLSService *ls_svr = MTL(ObLSService *);
    ASSERT_NE(nullptr, ls_svr);
    ObLSHandle handle;
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ObLSID(ObLSID::SYS_LS_ID), handle,
                                          ObLSGetMod::STORAGE_MOD));
    ObLS *ls = handle.get_ls();
    ASSERT_NE(nullptr, ls);
    bool elected = false;
    for (int i = 0; i < 1000; ++i) {
      ObRole role;
      int64_t proposal_id = 0;
      ASSERT_EQ(OB_SUCCESS,
                ls->get_log_handler()->get_role(role, proposal_id));
      if (ObRole::LEADER == role) {
        elected = true;
        break;
      }
      ob_usleep(10 * 1000);
    }
    ASSERT_TRUE(elected) << "user tenant SYS LS did not elect a leader within 10 s";
  }
  LOG_INFO("user tenant ready", K_(tenant_id));
}

// 并发度和合并类型组合只检查窗口，不重复执行昂贵的对象存储删除。
TEST_F(TestIVFFailGC, writer_windows_match_gc_slices)
{
  using namespace blocksstable;
  using namespace compaction;
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  for (const int64_t task_cnt : {1, 2, 4, 16, 128}) {
    for (const ObMergeType merge_type : {MAJOR_MERGE, MEDIUM_MERGE, INC_MAJOR_MERGE}) {
      SCOPED_TRACE(::testing::Message() << "task_cnt=" << task_cnt << " merge_type=" << merge_type);
      ObArenaAllocator allocator(ObMemAttr(tenant_id_, "IVFFailGCTest"));
      ObTabletMergeDagParam param(merge_type, ObLSID(1001), ObTabletID(210000), -1);
      ObSSAsyncIndexMergeCtx ctx(param, allocator);
      ctx.task_cnt_ = task_cnt;
      ObMacroSeqWindow data_window;
      ASSERT_EQ(OB_SUCCESS, ctx.get_index_tree_macro_start_seq(data_window.end_));
      ASSERT_EQ(task_cnt * MACRO_STEP_SIZE, data_window.end_);
      ctx.static_param_.is_ivf_index_ = true;
      ASSERT_EQ(OB_SUCCESS, ctx.generate_macro_seq_info(0, data_window.start_));
      ASSERT_EQ(OB_SUCCESS, ctx.get_index_tree_macro_start_seq(data_window.end_));
      ASSERT_EQ(ObIVFMergeSSTableWriter::OUTPUT_WRITER_COUNT, ctx.get_data_macro_seq_slot_count());
      ASSERT_EQ(ctx.get_data_macro_seq_slot_count() * MACRO_STEP_SIZE, data_window.end_);
      ASSERT_EQ(task_cnt, ctx.task_cnt_);
      for (int64_t i = 0; i < ObIVFMergeSSTableWriter::OUTPUT_WRITER_COUNT; ++i) {
        ObMacroSeqWindow window;
        ASSERT_EQ(OB_SUCCESS, ObIVFMergeSSTableWriter::build_macro_seq_window_(data_window, i, window));
        ASSERT_EQ(i * MACRO_STEP_SIZE, window.start_);
        ASSERT_EQ((i + 1) * MACRO_STEP_SIZE, window.end_);
      }
    }
  }
}

// 同 snapshot 重建分配不同构建 ID，完成较早轮次不能误完成较晚轮次。
TEST_F(TestIVFFailGC, non_idem_major_protocol)
{
  using namespace blocksstable;
  using namespace compaction;
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  const ObTabletID tablet_id(220000);
  const SCN reorg_scn = SCN::min_scn();
  SCN snapshot;
  ASSERT_EQ(OB_SUCCESS, snapshot.convert_for_tx(10000));
  ObSSMetaCtrlKey tablet_meta_key(ObStorageObjectType::SHARED_TABLET_META,
      tablet_id, reorg_scn, SCN::min_scn(), ls_id);
  ObSSMetaCtrlKey tablet_lock_key;
  ASSERT_EQ(OB_SUCCESS, tablet_meta_key.get_lock_row_key(tablet_lock_key));
  ObLockAtomicProtocol lock_row;
  ASSERT_EQ(OB_SUCCESS, lock_row.init(tenant_id_, ls_id, tablet_lock_key));
  ASSERT_EQ(OB_SUCCESS, lock_row.check_and_create_lock_row(sql_proxy()));

  ObAtomicProtocolType type;
  type.type_ = ObAtomicProtocolTypeEnum::MAJOR_SSTABLE;
  ObSSTableAtomicProtocol first;
  ObSSTableAtomicProtocol second;
  int64_t first_op = -1;
  int64_t second_op = -1;
  ASSERT_EQ(OB_SUCCESS, SSCompactHelper::start_non_idem_major_task(
      ls_id, tablet_id, reorg_scn, snapshot.get_val_for_tx(), 82, 0, first_op, first));
  ASSERT_EQ(OB_SUCCESS, SSCompactHelper::start_non_idem_major_task(
      ls_id, tablet_id, reorg_scn, snapshot.get_val_for_tx(), 82, 0, second_op, second));
  ASSERT_EQ(0, first_op);
  ASSERT_EQ(1, second_op);
  ObSSMetaCtrlKeyInfo first_key;
  ObSSMetaCtrlKeyInfo second_key;
  ASSERT_EQ(OB_SUCCESS, first.get_ctrl_key_info(first_key));
  ASSERT_EQ(OB_SUCCESS, second.get_ctrl_key_info(second_key));
  ASSERT_EQ(ObStorageObjectType::SHARED_NON_IDEM_MAJOR_SSTABLE_CTRL, first_key.key_.type_);
  ASSERT_EQ(ObStorageObjectType::SHARED_NON_IDEM_MAJOR_SSTABLE_CTRL, second_key.key_.type_);
  ASSERT_NE(first_key.key_.sstable_key_.control_scn_, second_key.key_.sstable_key_.control_scn_);
  ASSERT_EQ(OB_SUCCESS, SSCompactHelper::finish_non_idem_major_task(first));
  ASSERT_TRUE(first.is_status_complete());
  ASSERT_EQ(first_op, first.op_id_);
  ASSERT_TRUE(second.is_status_start());
  // 普通 Major 查询不能加载非幂等轮次；按精确控制键验证 second 未被 finish。
  ObSSMetaCtrlValue second_value;
  ASSERT_EQ(OB_SUCCESS, read_ctrl_(second_key, second_value));
  ASSERT_TRUE(second_value.ctrl_info_.status_.is_start_status());
  ASSERT_EQ(second_op, second_value.ctrl_info_.op_id_);
  ObSSTableAtomicProtocol loaded;
  ASSERT_EQ(OB_SUCCESS, loaded.init_for_load(type, tenant_id_, ls_id, tablet_id, reorg_scn));
  bool need_create = true;
  ASSERT_EQ(OB_SUCCESS, loaded.load_if_exist(snapshot.get_val_for_tx(), need_create));
  ASSERT_TRUE(need_create);
  ASSERT_FALSE(loaded.is_non_idem_major());
  ASSERT_EQ(OB_SUCCESS, second.abort());
  ASSERT_EQ(OB_STATE_NOT_MATCH, SSCompactHelper::finish_non_idem_major_task(second));

  // Like Mini/Minor, each non-idempotent attempt allocates a new op_id regardless of snapshot.
  SCN older_snapshot;
  ASSERT_EQ(OB_SUCCESS, older_snapshot.convert_for_tx(9999));
  ObSSTableAtomicProtocol older_attempt;
  int64_t op_id = -1;
  ASSERT_EQ(OB_SUCCESS, SSCompactHelper::start_non_idem_major_task(
      ls_id, tablet_id, reorg_scn, older_snapshot.get_val_for_tx(), 82, 0, op_id, older_attempt));
  ASSERT_GT(op_id, second_op);
  ASSERT_EQ(OB_SUCCESS, older_attempt.abort());

  for (const ObMergeType merge_type : {MEDIUM_MERGE, INC_MAJOR_MERGE}) {
    SCOPED_TRACE(merge_type);
    const bool inc_major = is_inc_major_merge(merge_type);
    ObSSTableAtomicProtocol protocol;
    ASSERT_EQ(OB_SUCCESS, inc_major
        ? SSCompactHelper::start_inc_major_compaction_task(
            ls_id, tablet_id, reorg_scn, snapshot.get_val_for_tx(), 82, 0, op_id, protocol)
        : SSCompactHelper::start_non_idem_major_task(
            ls_id, tablet_id, reorg_scn, snapshot.get_val_for_tx(), 82, 0, op_id, protocol));
    ASSERT_EQ(inc_major ? 0 : older_attempt.op_id_ + 1, op_id);
    ASSERT_EQ(op_id, protocol.op_id_);
    ASSERT_EQ(!inc_major, protocol.is_non_idem_major());
    ObSSMetaCtrlKeyInfo key;
    ASSERT_EQ(OB_SUCCESS, protocol.get_ctrl_key_info(key));
    ASSERT_EQ(inc_major ? ObStorageObjectType::SHARED_INC_MAJOR_SSTABLE_CTRL
                        : ObStorageObjectType::SHARED_NON_IDEM_MAJOR_SSTABLE_CTRL, key.key_.type_);
    ASSERT_EQ(OB_SUCCESS, inc_major
        ? SSCompactHelper::finish_inc_major_compaction_task(protocol)
        : SSCompactHelper::finish_non_idem_major_task(protocol));
    ASSERT_TRUE(protocol.is_status_complete());
  }
}

TEST_F(TestIVFFailGC, major_protocols_are_isolated)
{
  using namespace blocksstable;
  using namespace compaction;
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  const ObTabletID tablet_id(220010);
  const SCN reorg_scn = SCN::min_scn();
  SCN snapshot;
  ASSERT_EQ(OB_SUCCESS, snapshot.convert_for_tx(10000));
  ASSERT_NO_FATAL_FAILURE(ensure_tablet_lock_(tablet_id));
  ObAtomicProtocolType ordinary_type;
  ordinary_type.type_ = ObAtomicProtocolTypeEnum::MAJOR_SSTABLE;
  ObAtomicProtocolType independent_type;
  independent_type.type_ = ObAtomicProtocolTypeEnum::NON_IDEM_MAJOR_SSTABLE;
  ObSSMetaCtrlObjectInfo ordinary_info;
  ObSSMetaCtrlObjectInfo independent_info;
  ASSERT_EQ(OB_SUCCESS, ordinary_info.init_by_major(snapshot, MACRO_STEP_SIZE, 2, 0));
  ASSERT_EQ(OB_SUCCESS, independent_info.init_by_non_idem_major(snapshot, MACRO_STEP_SIZE, 2, 0));

  // Protocol admission and GC must reject mismatched metadata in both directions.
  ObSSTableAtomicProtocol invalid;
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid.init(
      ordinary_type, tenant_id_, ls_id, tablet_id, reorg_scn, independent_info));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid.init(
      independent_type, tenant_id_, ls_id, tablet_id, reorg_scn, ordinary_info));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid.init_for_load(
      independent_type, tenant_id_, ls_id, tablet_id, reorg_scn));
  ObArray<MacroBlockId> blocks;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSFailGCUtil::detect_sstable_block_ids(
      ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL, ls_id, tablet_id, reorg_scn,
      independent_info, 0, blocks));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSFailGCUtil::detect_sstable_block_ids(
      ObStorageObjectType::SHARED_NON_IDEM_MAJOR_SSTABLE_CTRL, ls_id, tablet_id, reorg_scn,
      ordinary_info, 0, blocks));
  ASSERT_TRUE(blocks.empty());

  ObSSTableAtomicProtocol independent;
  ASSERT_EQ(OB_SUCCESS, independent.init(
      independent_type, tenant_id_, ls_id, tablet_id, reorg_scn, independent_info));
  ASSERT_EQ(OB_SUCCESS, independent.create(false));
  ObSSTableAtomicProtocol ordinary;
  ASSERT_EQ(OB_SUCCESS, SSCompactHelper::start_major_task(
      ls_id, tablet_id, reorg_scn, 10000, 2, 0, ordinary));
  ASSERT_EQ(0, independent.op_id_);
  ASSERT_EQ(0, ordinary.op_id_);

  ObSSMetaCtrlKeyInfo ordinary_key;
  ObSSMetaCtrlKeyInfo independent_key;
  ASSERT_EQ(OB_SUCCESS, ordinary.get_ctrl_key_info(ordinary_key));
  ASSERT_EQ(OB_SUCCESS, independent.get_ctrl_key_info(independent_key));
  ASSERT_EQ(ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL, ordinary_key.key_.type_);
  ASSERT_NE(ordinary_key.key_.type_, independent_key.key_.type_);
  ObSSMetaCtrlKey ordinary_lock;
  ObSSMetaCtrlKey independent_lock;
  ASSERT_EQ(OB_SUCCESS, ordinary_key.key_.get_lock_row_key(ordinary_lock));
  ASSERT_EQ(OB_SUCCESS, independent_key.key_.get_lock_row_key(independent_lock));
  ASSERT_NE(ordinary_lock.convert_to_macro_id(), independent_lock.convert_to_macro_id());
  ASSERT_EQ(ObStorageObjectType::SHARED_MAJOR_LOCK, ordinary_lock.type_);
  ASSERT_EQ(ObStorageObjectType::SHARED_NON_IDEM_MAJOR_LOCK, independent_lock.type_);

  // A newer independent attempt must not replace the ordinary snapshot's row.
  ObSSTableAtomicProtocol next_attempt;
  ASSERT_EQ(OB_SUCCESS, next_attempt.init(
      independent_type, tenant_id_, ls_id, tablet_id, reorg_scn, independent_info));
  ASSERT_EQ(OB_SUCCESS, next_attempt.create(false));
  ASSERT_EQ(independent.op_id_ + 1, next_attempt.op_id_);
  ObSSTableAtomicProtocol reused;
  ASSERT_EQ(OB_SUCCESS, reused.init(
      ordinary_type, tenant_id_, ls_id, tablet_id, reorg_scn, ordinary_info));
  ASSERT_EQ(OB_SUCCESS, reused.create(false));
  ASSERT_EQ(ordinary.op_id_, reused.op_id_);
  ASSERT_EQ(OB_SUCCESS, SSCompactHelper::finish_major_task(
      ls_id, tablet_id, reorg_scn, 10000, ordinary));
  ObSSMetaCtrlValue value;
  ASSERT_EQ(OB_SUCCESS, read_ctrl_(ordinary_key, value));
  ASSERT_TRUE(value.ctrl_info_.status_.is_complete_status());
  ASSERT_EQ(OB_SUCCESS, read_ctrl_(independent_key, value));
  ASSERT_TRUE(value.ctrl_info_.status_.is_start_status());
  ASSERT_EQ(OB_SUCCESS, independent.abort());
  ASSERT_EQ(OB_SUCCESS, next_attempt.abort());

  // Snapshot-based admission belongs only to the ordinary Major protocol.
  for (const bool newer_is_independent : {false, true}) {
    const ObTabletID other_tablet(220011 + newer_is_independent);
    ASSERT_NO_FATAL_FAILURE(ensure_tablet_lock_(other_tablet));
    SCN newer_snapshot;
    ASSERT_EQ(OB_SUCCESS, newer_snapshot.convert_for_tx(20000));
    ObSSMetaCtrlObjectInfo newer_info;
    ASSERT_EQ(OB_SUCCESS, newer_is_independent
        ? newer_info.init_by_non_idem_major(newer_snapshot, MACRO_STEP_SIZE, 2, 0)
        : newer_info.init_by_major(newer_snapshot, MACRO_STEP_SIZE, 2, 0));
    ObSSTableAtomicProtocol newer;
    ASSERT_EQ(OB_SUCCESS, newer.init(newer_is_independent ? independent_type : ordinary_type,
        tenant_id_, ls_id, other_tablet, reorg_scn, newer_info));
    ASSERT_EQ(OB_SUCCESS, newer.create(false));
    ObSSTableAtomicProtocol older_attempt;
    ASSERT_EQ(OB_SUCCESS, older_attempt.init(newer_is_independent ? ordinary_type : independent_type,
        tenant_id_, ls_id, other_tablet, reorg_scn,
        newer_is_independent ? ordinary_info : independent_info));
    ASSERT_EQ(OB_SUCCESS, older_attempt.create(false));
    ASSERT_EQ(OB_SUCCESS, older_attempt.abort());
    ASSERT_EQ(OB_SUCCESS, newer.abort());
  }
}

// 非幂等 Major 的失败对象按轮次隔离，不受普通 Major 的 snapshot 复用规则限制。
TEST_F(TestIVFFailGC, non_idem_major_gc_eligibility)
{
  using namespace blocksstable;
  using namespace compaction;
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ObSSFailGCExecutorTask executor;
  ASSERT_EQ(OB_SUCCESS, executor.fail_gc_tablet_map_.create(4, ObMemAttr(tenant_id_, "IVFFailGCTest")));
  ObSSFailGCDagCtx ctx;
  ctx.tenant_id_ = tenant_id_;
  SCN snapshot;
  ASSERT_EQ(OB_SUCCESS, snapshot.convert_for_tx(10000));
  ObSSMetaCtrlKeyInfo key;
  key.tenant_id_ = tenant_id_;
  key.ls_id_ = ObLSID(1001);
  const ObSSMetaCtrlKey ctrl_key(ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL,
      ObTabletID(220001), SCN::min_scn(), snapshot, key.ls_id_);
  ASSERT_EQ(OB_SUCCESS, key.key_.assign(ctrl_key));
  ObSSMetaCtrlValue value;
  ASSERT_EQ(OB_SUCCESS, value.ctrl_info_.status_.set_status_start());
  ASSERT_EQ(OB_SUCCESS, value.ctrl_info_.status_.set_status_abandon());
  const ObSSGCTabletBaseInfo tablet_info(ObTabletID(220001), SCN::min_scn());
  for (const bool published : {false, true}) {
    SCN last_snapshot;
    ASSERT_EQ(OB_SUCCESS, last_snapshot.convert_for_tx(published ? 10000 : 9999));
    ASSERT_EQ(OB_SUCCESS, executor.fail_gc_tablet_map_.set_refactored(
        tablet_info, ObSSFailGCTabletMapValue(false, last_snapshot), 1));
    for (const bool is_non_idem : {false, true}) {
      key.key_.type_ = is_non_idem
          ? ObStorageObjectType::SHARED_NON_IDEM_MAJOR_SSTABLE_CTRL
          : ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL;
      ASSERT_EQ(OB_SUCCESS, is_non_idem
          ? value.object_info_.init_by_non_idem_major(snapshot, MACRO_STEP_SIZE, 82, 0)
          : value.object_info_.init_by_major(snapshot, MACRO_STEP_SIZE, 82, 0));
      bool collect = false;
      bool generate = false;
      ASSERT_EQ(OB_SUCCESS, executor.check_need_collect_abandoned_row_(ctx, key, value, collect));
      ASSERT_EQ(is_non_idem || published, collect);
      ASSERT_EQ(OB_SUCCESS, executor.check_need_generate_task_(key, value, generate));
      ASSERT_EQ(is_non_idem, generate);
    }
  }
}

// 相同 snapshot 的实际对象写入使用不同 payload，确保命名空间复用会在重试写处暴露。
// COMPLETE 分支刻意停在 table store 发布前，随后用真实 executor 回收遗留控制行。
static void check_fast_failed_attempt(TestIVFFailGC &test, const bool complete)
{
  using namespace blocksstable;
  using namespace compaction;
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(TestIVFFailGC::tenant_id_));
  const ObTabletID tablet_id(complete ? 230002 : 230001);
  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  const int64_t snapshot = 10000;
  ASSERT_NO_FATAL_FAILURE(test.ensure_tablet_lock_(tablet_id));
  ObArenaAllocator allocator;
  ObTabletMergeDagParam param(MEDIUM_MERGE, ls_id, tablet_id, -1);
  ObSSAsyncIndexMergeCtx failed_ctx(param, allocator);
  ObSSTableAtomicProtocol &failed = failed_ctx.sstable_protocol_;
  int64_t failed_op = -1;
  ASSERT_EQ(OB_SUCCESS, SSCompactHelper::start_non_idem_major_task(
      ls_id, tablet_id, SCN::min_scn(), snapshot, 2, 0, failed_op, failed));
  ObSSMetaCtrlKeyInfo failed_key;
  ASSERT_EQ(OB_SUCCESS, failed.get_ctrl_key_info(failed_key));
  MacroBlockId failed_ids[2];
  MacroBlockId retry_ids[2];
  const ObStorageObjectType types[] = {ObStorageObjectType::SHARED_NON_IDEM_MAJOR_DATA_MACRO,
                                      ObStorageObjectType::SHARED_NON_IDEM_MAJOR_META_MACRO};
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, test.build_sstable_macro_id_(types[i], ls_id, tablet_id,
        failed_op, i * MACRO_STEP_SIZE, 0, SCN::min_scn(), failed_ids[i]));
    ASSERT_NO_FATAL_FAILURE(test.write_macro_block_(failed_ids[i], 'a'));
  }
  if (complete) {
    ASSERT_EQ(OB_SUCCESS, SSCompactHelper::finish_non_idem_major_task(failed));
  }
  failed_ctx.destroy();
  EXPECT_EQ(complete, failed.is_status_complete());
  EXPECT_EQ(!complete, failed.is_status_abandon());
  ObSSMetaCtrlValue expected;
  ASSERT_EQ(OB_SUCCESS, test.read_ctrl_(failed_key, expected));
  EXPECT_EQ(complete, expected.ctrl_info_.status_.is_complete_status());
  EXPECT_EQ(!complete, expected.ctrl_info_.status_.is_abandon_status());

  ObSSTableAtomicProtocol retry;
  int64_t retry_op = -1;
  ASSERT_EQ(OB_SUCCESS, SSCompactHelper::start_non_idem_major_task(
      ls_id, tablet_id, SCN::min_scn(), snapshot, 2, 0, retry_op, retry));
  ASSERT_NE(failed_op, retry_op);
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, test.build_sstable_macro_id_(types[i], ls_id, tablet_id,
        retry_op, i * MACRO_STEP_SIZE, 0, SCN::min_scn(), retry_ids[i]));
    ASSERT_NE(failed_ids[i], retry_ids[i]);
    ASSERT_NO_FATAL_FAILURE(test.write_macro_block_(retry_ids[i], 'b'));
  }
  ASSERT_EQ(OB_SUCCESS, SSCompactHelper::finish_non_idem_major_task(retry));
  ObSSMetaCtrlKeyInfo retry_key;
  ASSERT_EQ(OB_SUCCESS, retry.get_ctrl_key_info(retry_key));
  ObSharedGCTask task;
  ASSERT_NO_FATAL_FAILURE(test.generate_orphan_gc_(failed_key, expected, task, allocator));
  ASSERT_NO_FATAL_FAILURE(test.execute_gc_task_(task));
  // 重跑同一回收范围，已完成的任务不得重复认领或影响下一轮。
  ASSERT_NO_FATAL_FAILURE(test.execute_gc_task_(task));
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_NO_FATAL_FAILURE(test.assert_block_exists_(failed_ids[i], false));
    ASSERT_NO_FATAL_FAILURE(test.assert_block_exists_(retry_ids[i], true));
  }
  ObSSMetaCtrlValue retry_value;
  ASSERT_EQ(OB_SUCCESS, test.read_ctrl_(retry_key, retry_value));
  ASSERT_TRUE(retry_value.ctrl_info_.status_.is_complete_status());
  ASSERT_EQ(retry_op, retry_value.ctrl_info_.op_id_);
  // 清理由本用例创建且尚未发布的重试轮次，避免干扰同进程其他用例。
  ASSERT_EQ(OB_SUCCESS, retry.abort());
  ASSERT_EQ(OB_SUCCESS, test.read_ctrl_(retry_key, retry_value));
  ObSharedGCTask retry_gc;
  ASSERT_NO_FATAL_FAILURE(test.generate_orphan_gc_(retry_key, retry_value, retry_gc, allocator));
  ASSERT_NO_FATAL_FAILURE(test.execute_gc_task_(retry_gc));
}

TEST_F(TestIVFFailGC, partial_write_retry_generates_gc)
{
  FastCaseBudget budget;
  ASSERT_NO_FATAL_FAILURE(check_fast_failed_attempt(*this, false));
  ASSERT_LT(budget.elapsed_us(), FAST_CASE_TIMEOUT_US);
}

TEST_F(TestIVFFailGC, complete_without_publish_generates_gc)
{
  FastCaseBudget budget;
  ASSERT_NO_FATAL_FAILURE(check_fast_failed_attempt(*this, true));
  ASSERT_LT(budget.elapsed_us(), FAST_CASE_TIMEOUT_US);
}

// 用真实共享宏块构造很小的 IVF SSTable；关闭 small-sstable 优化保证宏块可独立回收。
// 不调用训练器，payload 只用于验证存储读写和引用关系。
class TinySharedIVF
{
public:
  TinySharedIVF() : allocator_("TinySharedIVF"), builder_(false) {}

  void build(const uint64_t tenant_id, const ObTabletID &tablet_id,
      const int64_t op_id, const int64_t snapshot)
  {
    using namespace blocksstable;
    using namespace share::schema;
    schema_.set_tenant_id(tenant_id);
    schema_.set_tablegroup_id(1);
    schema_.set_database_id(1);
    schema_.set_table_id(tablet_id.id());
    schema_.set_data_table_id(tablet_id.id());
    schema_.set_table_type(USER_INDEX);
    schema_.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
    ASSERT_EQ(OB_SUCCESS, schema_.set_table_name("ivf_gc_reuse"));
    schema_.set_rowkey_column_num(3);
    schema_.set_max_used_column_id(OB_APP_MIN_COLUMN_ID + 3);
    schema_.set_block_size(16 * 1024);
    ASSERT_EQ(OB_SUCCESS, schema_.set_compress_func_name("none"));
    schema_.set_row_store_type(CS_ENCODING_ROW_STORE);
    schema_.set_pctfree(10);
    const char *names[] = {"type", "cid", "vid", "data"};
    for (int64_t i = 0; i < 4; ++i) {
      ObColumnSchemaV2 column;
      column.set_table_id(tablet_id.id());
      column.set_column_id(OB_APP_MIN_COLUMN_ID + i);
      ASSERT_EQ(OB_SUCCESS, column.set_column_name(names[i]));
      column.set_data_type(3 == i ? ObVarcharType : ObIntType);
      column.set_collation_type(CS_TYPE_BINARY);
      column.set_data_length(3 == i ? 1024 : 0);
      column.set_rowkey_position(3 == i ? 0 : i + 1);
      ASSERT_EQ(OB_SUCCESS, schema_.add_column(column));
    }
    ASSERT_EQ(OB_SUCCESS, storage_schema_.init(allocator_, schema_, lib::Worker::CompatMode::MYSQL));
    SCN end_scn;
    ASSERT_EQ(OB_SUCCESS, end_scn.convert_for_tx(snapshot));
    ASSERT_EQ(OB_SUCCESS, desc_.init(false, schema_, ObLSID(ObLSID::SYS_LS_ID),
        tablet_id, compaction::MEDIUM_MERGE, snapshot, DATA_CURRENT_VERSION,
        false, 0, 1, SCN::min_scn(), end_scn));
    desc_.get_static_desc().is_non_idem_major_ = true;
    desc_.get_static_desc().op_id_ = op_id;
    desc_.get_static_desc().schema_version_ = 10;
    desc_.get_static_desc().exec_mode_ = compaction::EXEC_MODE_OUTPUT;
    desc_.get_static_desc().io_write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;
    desc_.get_desc().sstable_index_builder_ = &builder_;
    ASSERT_EQ(OB_SUCCESS, builder_.init(desc_.get_desc(), ObSSTableIndexBuilder::DISABLE));
    builder_.set_is_async_index(true);
    ObIvfSSTableMeta meta;
    meta.dim_ = 8;
    meta.index_type_ = VIAT_DISK_BBQ;
    meta.dist_type_ = VIDA_L2;
    meta.format_version_ = IVF_SSTABLE_FORMAT_VERSION;
    meta.nbits_ = 1;
    meta.fht_algo_version_ = quantization::ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION;
    meta.centroid_count_ = 1;
    ASSERT_TRUE(meta.is_valid());
    ASSERT_EQ(OB_SUCCESS, builder_.set_ivf_meta(meta));
    ObIvfMacroBlockWriter writer;
    ObMacroSeqParam seq;
    seq.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    seq.start_ = 0;
    seq.end_ = compaction::MACRO_STEP_SIZE;
    ASSERT_NE(nullptr, builder_.get_object_cleaner());
    ASSERT_EQ(OB_SUCCESS, writer.open(desc_.get_desc(), 0, seq,
        ObPreWarmerParam(PRE_WARM_TYPE_NONE), *builder_.get_object_cleaner()));
    ObDatumRow row;
    ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 6));
    // 两行足以验证实际 append_macro_block 复用；输出无需读取/训练向量。
    for (const int64_t type : {IVF_ROW_TYPE_CENTROID, IVF_ROW_TYPE_RAW_VECTOR}) {
      row.storage_datums_[0].set_int(type);
      row.storage_datums_[1].set_int(0);
      row.storage_datums_[2].set_int(0);
      row.storage_datums_[3].set_int(-snapshot);
      row.storage_datums_[4].set_int(0);
      row.storage_datums_[5].set_string(ObString::make_string("gc-reuse-payload"));
      row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      row.mvcc_row_flag_.set_last_multi_version_row(true);
      ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
    }
    ASSERT_EQ(OB_SUCCESS, writer.close());
    int64_t index_seq = compaction::MACRO_STEP_SIZE;
    ASSERT_EQ(OB_SUCCESS, builder_.close_with_macro_seq(res_, index_seq,
        OB_DEFAULT_MACRO_BLOCK_SIZE, 0, ObPreWarmerParam(PRE_WARM_TYPE_NONE)));
    ObITable::TableKey key;
    key.table_type_ = ObITable::IVF_INDEX_SSTABLE;
    key.tablet_id_ = tablet_id;
    key.version_range_.base_version_ = 0;
    key.version_range_.snapshot_version_ = snapshot;
    ObTabletCreateSSTableParam param;
    ASSERT_EQ(OB_SUCCESS, param.init_for_ss_ddl(res_, key, storage_schema_, 10,
        DIRECT_LOAD_DDL, transaction::ObTransID(), transaction::ObTxSEQ()));
    ASSERT_EQ(OB_SUCCESS, table_.init(param, &allocator_));
    ASSERT_FALSE(table_.is_small_sstable());
    ASSERT_GT(res_.data_block_ids_.count(), 0);
  }

  ObArenaAllocator allocator_;
  share::schema::ObTableSchema schema_;
  ObStorageSchema storage_schema_;
  blocksstable::ObWholeDataStoreDesc desc_;
  blocksstable::ObSSTableIndexBuilder builder_;
  blocksstable::ObSSTableMergeRes res_;
  blocksstable::ObIvfSSTable table_;
};


TEST_F(TestIVFFailGC, empty_delta_reuse_survives_gc)
{
  using namespace blocksstable;
  using namespace compaction;
  FastCaseBudget budget;
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  const ObTabletID tablet_id(230003);
  TinySharedIVF source;
  TinySharedIVF obsolete;
  ASSERT_NO_FATAL_FAILURE(source.build(tenant_id_, tablet_id, 10, 10000));
  ASSERT_NO_FATAL_FAILURE(obsolete.build(tenant_id_, tablet_id, 11, 9999));
  ObArenaAllocator allocator;
  ObIvfSSTable empty;
  ObTabletCreateSSTableParam empty_param;
  ASSERT_EQ(OB_SUCCESS, empty_param.init_for_empty_major_sstable(
      tablet_id, source.storage_schema_, 10001, -1, false, false));
  empty_param.table_key_.table_type_ = ObITable::INC_IVF_INDEX_SSTABLE;
  empty_param.table_key_.version_range_.base_version_ = 10000;
  empty_param.table_key_.version_range_.snapshot_version_ = 10001;
  empty_param.ivf_meta_.set_empty();
  ASSERT_EQ(OB_SUCCESS, empty_param.uncommit_tx_info_.push_back(
      ObUncommitTxDesc(1, 10001, ObUncommitTxDesc::COMMIT_VERSION)));
  empty_param.uncommit_tx_info_.set_info_status_record_seq();
  ASSERT_EQ(OB_SUCCESS, empty.init(empty_param, &allocator));

  ObTabletMergeDagParam merge_param(MEDIUM_MERGE, ObLSID(ObLSID::SYS_LS_ID), tablet_id, -1);
  merge_param.merge_version_ = 10001;
  ObSSAsyncIndexMergeCtx ctx(merge_param, allocator);
  ctx.task_cnt_ = 1;
  ctx.parallel_merge_ctx_.concurrent_cnt_ = 1;
  ctx.static_param_.concurrent_cnt_ = 1;
  ctx.static_param_.is_ivf_index_ = true;
  ctx.static_param_.rec_scn_.set_min();
  ctx.static_param_.sstable_logic_seq_ = 0;
  ctx.static_param_.version_range_.base_version_ = 0;
  ctx.static_param_.version_range_.snapshot_version_ = 10001;
  ObStorageSchema *merge_schema = OB_NEWx(ObStorageSchema, &allocator);
  ASSERT_NE(nullptr, merge_schema);
  ASSERT_EQ(OB_SUCCESS, merge_schema->init(
      allocator, source.schema_, lib::Worker::CompatMode::MYSQL));
  ctx.static_param_.schema_ = merge_schema;
  ASSERT_EQ(OB_SUCCESS, ctx.read_info_.init(allocator, 4, 3, false,
      source.desc_.get_desc().get_full_stored_col_descs()));
  ObTableHandleV2 base_handle;
  ObTableHandleV2 empty_handle;
  ASSERT_EQ(OB_SUCCESS, base_handle.set_sstable_with_tablet(&source.table_));
  ASSERT_EQ(OB_SUCCESS, empty_handle.set_sstable_with_tablet(&empty));
  ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(base_handle));
  ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(empty_handle));
  ObRoaringBitmap *bitmap = OB_NEWx(ObRoaringBitmap, &allocator, &allocator);
  ASSERT_NE(nullptr, bitmap);
  ASSERT_EQ(OB_SUCCESS, ctx.deletion_bitmap_handle_.init_from_bitmap(allocator, bitmap));
  ObIVFMergeInputAdapter input(allocator);
  input.index_table_id_ = tablet_id.id();
  input.index_tablet_id_ = tablet_id;
  input.vector_dim_ = 8;
  input.max_sample_count_ = 16;
  input.dist_type_ = VIDA_L2;
  ASSERT_EQ(OB_SUCCESS, input.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
  ASSERT_EQ(OB_SUCCESS, input.prepare_source_(ctx));
  auto &desc = ctx.get_merge_info().get_sstable_build_desc();
  auto *builder = ctx.get_merge_info().get_index_builder();
  SCN end_scn;
  ASSERT_EQ(OB_SUCCESS, end_scn.convert_for_tx(10001));
  ASSERT_EQ(OB_SUCCESS, desc.init(false, source.schema_, ObLSID(ObLSID::SYS_LS_ID),
      tablet_id, MEDIUM_MERGE, 10001, DATA_CURRENT_VERSION, false, 0, 1, SCN::min_scn(), end_scn));
  desc.get_static_desc().is_non_idem_major_ = true;
  desc.get_static_desc().op_id_ = 12;
  desc.get_static_desc().schema_version_ = 10;
  desc.get_static_desc().exec_mode_ = EXEC_MODE_OUTPUT;
  desc.get_static_desc().io_write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;
  desc.get_desc().sstable_index_builder_ = builder;
  ASSERT_EQ(OB_SUCCESS, builder->init(desc.get_desc(), ObSSTableIndexBuilder::DISABLE));
  builder->set_is_async_index(true);
  ObIVFMergeDagCtx reuse;
  reuse.input_adapter_ = &input;
  DEFER(reuse.input_adapter_ = nullptr);
  bool reused = false;
  ASSERT_EQ(OB_SUCCESS, reuse.try_reuse_input_(ctx, reused, false /*single_inc*/));
  ASSERT_TRUE(reused);
  ObSSTableMergeRes output;
  int64_t index_seq = ctx.get_data_macro_seq_slot_count() * MACRO_STEP_SIZE;
  ASSERT_EQ(OB_SUCCESS, builder->close_with_macro_seq(output, index_seq,
      OB_DEFAULT_MACRO_BLOCK_SIZE, 0, ObPreWarmerParam(PRE_WARM_TYPE_NONE)));
  ASSERT_EQ(source.res_.data_block_ids_.count(), output.data_block_ids_.count());
  ASSERT_EQ(source.res_.data_blocks_cnt_, output.use_old_macro_block_count_);
  for (int64_t i = 0; i < output.data_block_ids_.count(); ++i) {
    ASSERT_EQ(source.res_.data_block_ids_.at(i), output.data_block_ids_.at(i));
  }
  ObTabletCreateSSTableParam published_param;
  ASSERT_EQ(OB_SUCCESS, published_param.init_for_merge(ctx, output, nullptr, 0));
  ObIvfSSTable published;
  ASSERT_EQ(OB_SUCCESS, published.init(published_param, &allocator));
  ASSERT_EQ(10001, published.get_snapshot_version());

  // DEL 来自旧 SSTable，KEPT 来自成功输出的实际宏块列表；额外淘汰一个 SSTable
  // 作为删除对照，防止 GC 空跑也通过。只驱动生产排序/差集/删除，不等待版本淘汰周期。
  ObGCMacroBlockCompare compare;
  ObMarkSweepPipeline pipeline;
  ASSERT_EQ(OB_SUCCESS, pipeline.init(tenant_id_, ObExternalSortConstant::MIN_MEMORY_LIMIT, 64L << 10,
      budget.deadline_us(), &compare));
  for (const ObMarkSweepGroupType group : {ObMarkSweepGroupType::DEL, ObMarkSweepGroupType::KEPT}) {
    ObMarkSweepSorter sorter;
    ASSERT_EQ(OB_SUCCESS, sorter.init(&pipeline));
    const ObSSTableMergeRes &res = ObMarkSweepGroupType::DEL == group ? source.res_ : output;
    for (int64_t i = 0; i < res.data_block_ids_.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, sorter.add_item(ObGCMacroBlockItem(res.data_block_ids_.at(i))));
    }
    for (int64_t i = 0; i < res.other_block_ids_.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, sorter.add_item(ObGCMacroBlockItem(res.other_block_ids_.at(i))));
    }
    if (ObMarkSweepGroupType::DEL == group) {
      for (int64_t i = 0; i < obsolete.res_.data_block_ids_.count(); ++i) {
        ASSERT_EQ(OB_SUCCESS, sorter.add_item(ObGCMacroBlockItem(obsolete.res_.data_block_ids_.at(i))));
      }
    }
    ObSEArray<ObSortedFileInfo, 1> files;
    ASSERT_EQ(OB_SUCCESS, sorter.finish(files));
    ASSERT_EQ(1, files.count());
    ASSERT_EQ(OB_SUCCESS, pipeline.add_file(group, files.at(0)));
  }
  ASSERT_EQ(OB_SUCCESS, pipeline.prepare_merge());
  ASSERT_EQ(OB_SUCCESS, pipeline.complete_group(ObMarkSweepGroupType::DEL));
  ASSERT_EQ(OB_SUCCESS, pipeline.complete_group(ObMarkSweepGroupType::KEPT));
  ASSERT_EQ(OB_SUCCESS, pipeline.complete_merge());
  ObSharedGCMacroIdList deleted;
  const ObGCMacroBlockItem *item = nullptr;
  int iter_ret = OB_SUCCESS;
  while (OB_SUCCESS == (iter_ret = pipeline.get_next_diff_item(item))) {
    ASSERT_EQ(OB_SUCCESS, deleted.add(item->macro_id_));
  }
  ASSERT_EQ(OB_ITER_END, iter_ret);
  ASSERT_GE(deleted.count(), obsolete.res_.data_block_ids_.count());
  const int64_t round_id = 230003;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskUtil::generate_mark_sweep_gc_task(
      tenant_id_, round_id, deleted, sql_proxy()));
  ObSharedGCTaskTableRangeIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC,
      round_id, ObSSGCTaskIdRange(0, INT64_MAX), sql_proxy()));
  ObSharedGCTask task;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, task));
  ASSERT_NO_FATAL_FAILURE(execute_gc_task_(task));
  ASSERT_EQ(OB_ITER_END, iter.get_next(allocator, task));
  for (int64_t i = 0; i < output.data_block_ids_.count(); ++i) {
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(output.data_block_ids_.at(i), true));
  }
  for (int64_t i = 0; i < obsolete.res_.data_block_ids_.count(); ++i) {
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(obsolete.res_.data_block_ids_.at(i), false));
  }
  // 已删除旧独有对象后仍能遍历成功输出，且宏块 ID 保持为基线的物理 ID。
  ObDatumRange range;
  range.set_whole_range();
  ObIMacroBlockIterator *macro_iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, published.scan_macro_block(range, *input.read_info_, allocator,
      macro_iter, false, true, true));
  ASSERT_NE(nullptr, macro_iter);
  DEFER(macro_iter->~ObIMacroBlockIterator());
  ObDataMacroBlockMeta macro_meta;
  ObMacroBlockDesc macro_desc;
  macro_desc.macro_meta_ = &macro_meta;
  int64_t macro_count = 0;
  while (OB_SUCCESS == (iter_ret = macro_iter->get_next_macro_block(macro_desc))) {
    ASSERT_LT(macro_count, output.data_block_ids_.count());
    ASSERT_EQ(output.data_block_ids_.at(macro_count++), macro_meta.get_macro_id());
  }
  ASSERT_EQ(OB_ITER_END, iter_ret);
  ASSERT_EQ(output.data_block_ids_.count(), macro_count);
  ASSERT_EQ(source.table_.get_row_count(), published.get_row_count());
  ASSERT_LT(budget.elapsed_us(), FAST_CASE_TIMEOUT_US);
}

// 普通 Major、非幂等 Major 和 Inc Major 均须枚举到末尾 writer，且不包含其他构建。
// 全窗口只验证 GC 枚举边界；真实删除由快速 GC 用例和 obtest 切主用例覆盖。
TEST_F(TestIVFFailGC, ivf_writer_windows_gc_detection)
{
  using namespace blocksstable;
  using namespace compaction;
  ASSERT_TRUE(is_user_tenant(tenant_id_));
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  const SCN reorg_scn = SCN::min_scn();
  SCN snapshot;
  snapshot.convert_for_logservice(10000);
  const int64_t op_id = 0;
  constexpr int64_t OBJECT_COUNT = 5;
  const char *modes[] = {"major", "inc-major-compaction", "non-idempotent-major"};
  const int64_t writer_indices[] = {0, 1, ObIVFMergeSSTableWriter::MAX_QUANT_TASK_COUNT + 1,
      ObIVFMergeSSTableWriter::OUTPUT_WRITER_COUNT - 1};
  for (int64_t path = 0; path < 3; ++path) {
    SCOPED_TRACE(modes[path]);
    const bool inc_major = 1 == path;
    const ObLSID ls_id(1001);
    const ObTabletID tablet_id(210000 + path);
    const int64_t fourth_id = inc_major || 2 == path ? op_id : snapshot.get_val_for_tx();
    const auto data_type = inc_major ? ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_DATA_MACRO
                                     : 2 == path ? ObStorageObjectType::SHARED_NON_IDEM_MAJOR_DATA_MACRO
                                                 : ObStorageObjectType::SHARED_MAJOR_DATA_MACRO;
    const auto meta_type = inc_major ? ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_META_MACRO
                                     : 2 == path ? ObStorageObjectType::SHARED_NON_IDEM_MAJOR_META_MACRO
                                                 : ObStorageObjectType::SHARED_MAJOR_META_MACRO;
    const auto ctrl_type = inc_major ? ObStorageObjectType::SHARED_INC_MAJOR_SSTABLE_CTRL
        : 2 == path ? ObStorageObjectType::SHARED_NON_IDEM_MAJOR_SSTABLE_CTRL
                    : ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL;
    ObArenaAllocator allocator(ObMemAttr(tenant_id_, "IVFFailGCTest"));
    ObTabletMergeDagParam param(inc_major ? INC_MAJOR_MERGE : MAJOR_MERGE, ls_id, tablet_id, -1);
    ObSSAsyncIndexMergeCtx ctx(param, allocator);
    ctx.task_cnt_ = 1;
    ctx.static_param_.is_ivf_index_ = true;
    ObMacroSeqWindow data_window;
    ASSERT_EQ(OB_SUCCESS, ctx.generate_macro_seq_info(0, data_window.start_));
    ASSERT_EQ(OB_SUCCESS, ctx.get_index_tree_macro_start_seq(data_window.end_));
    const int64_t parallel_cnt = ctx.get_data_macro_seq_slot_count() + INDEX_TREE_MACRO_SEQ_SLOT_CNT;
    ObSSMetaCtrlObjectInfo info;
    ASSERT_EQ(OB_SUCCESS, inc_major
        ? info.init_by_inc_major(snapshot, MACRO_STEP_SIZE, parallel_cnt, 0)
        : 2 == path
            ? info.init_by_non_idem_major(snapshot, MACRO_STEP_SIZE, parallel_cnt, 0)
            : info.init_by_major(snapshot, MACRO_STEP_SIZE, parallel_cnt, 0));
    MacroBlockId ids[OBJECT_COUNT];
    for (int64_t i = 0; i < OBJECT_COUNT; ++i) {
      int64_t seq = data_window.end_;
      if (i < OBJECT_COUNT - 1) {
        ObMacroSeqWindow window;
        ASSERT_EQ(OB_SUCCESS, ObIVFMergeSSTableWriter::build_macro_seq_window_(
            data_window, writer_indices[i], window));
        seq = window.start_;
      }
      ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(i == OBJECT_COUNT - 1 ? meta_type : data_type,
          ls_id, tablet_id, fourth_id, seq, 0 /*cg*/, reorg_scn, ids[i]));
      ASSERT_NO_FATAL_FAILURE(write_macro_block_(ids[i]));
      ASSERT_NO_FATAL_FAILURE(assert_block_exists_(ids[i], true));
    }
    MacroBlockId other_build;
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(data_type, ls_id, tablet_id,
        fourth_id + 1, ids[1].seq_id(), 0 /*cg*/, reorg_scn, other_build));
    ASSERT_NO_FATAL_FAILURE(write_macro_block_(other_build));
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(other_build, true));

    ObArray<MacroBlockId> candidates;
    ASSERT_EQ(OB_SUCCESS, ObSSFailGCUtil::detect_sstable_block_ids(
        ctrl_type, ls_id, tablet_id, reorg_scn, info, op_id, candidates));
    bool found[OBJECT_COUNT] = {false};
    bool contains_other_build = false;
    for (int64_t i = 0; i < candidates.count(); ++i) {
      const MacroBlockId &candidate = candidates.at(i);
      contains_other_build |= candidate == other_build;
      for (int64_t j = 0; j < OBJECT_COUNT; ++j) {
        found[j] |= candidate == ids[j];
      }
    }
    for (int64_t i = 0; i < OBJECT_COUNT; ++i) {
      ASSERT_TRUE(found[i]) << "missing writer object " << i;
    }
    ASSERT_FALSE(contains_other_build);
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int64_t c = 0;
  char *log_level = (char *)"INFO";
  char buf[1000];
  const auto env_or_default = [](const char *name, const char *fallback) {
    const char *value = std::getenv(name);
    return value != nullptr && value[0] != '\0' ? value : fallback;
  };
  const int64_t cur_time_ns = oceanbase::common::ObTimeUtility::current_time_ns();
  memset(buf, 0, sizeof(buf));
  oceanbase::common::databuff_printf(
      buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s"
      "&max_iops=10000&max_bandwidth=200000000B&scope=region",
      env_or_default("OB_TEST_S3_BUCKET", oceanbase::unittest::S3_BUCKET), cur_time_ns,
      env_or_default("OB_TEST_S3_ENDPOINT", oceanbase::unittest::S3_ENDPOINT),
      env_or_default("OB_TEST_S3_AK", oceanbase::unittest::S3_AK),
      env_or_default("OB_TEST_S3_SK", oceanbase::unittest::S3_SK),
      env_or_default("OB_TEST_S3_REGION", oceanbase::unittest::S3_REGION));
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
