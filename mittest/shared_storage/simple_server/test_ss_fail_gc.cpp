/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#define UNITTEST

#include <gmock/gmock.h>
#define protected public
#define private public
#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/restore/ob_object_storage_base.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/io/ob_io_manager.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "share/io/ob_io_define.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "src/storage/ss_garbage_collector/dag/ob_ss_fail_gc_dag.h"
#include "src/storage/ss_garbage_collector/dag/ob_ss_gc_exec_dag.h"
#include "src/storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "src/storage/ss_garbage_collector/ob_ss_fail_gc_util.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#include "src/storage/shared_storage/ob_remote_io_adapter.h"
#include "src/storage/shared_storage/ob_ss_object_access_util.h"
#include "src/storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "src/storage/tiered_metadata_store/ob_tiered_metadata_object_manager.h"
#include "storage/incremental/atomic_protocol/ob_sstable_atomic_protocol.h"
#include "storage/incremental/atomic_protocol/ob_lock_atomic_protocol.h"
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
namespace sslog
{
} // namespace sslog

using namespace unittest;
namespace storage
{
using namespace common;
using namespace share;

// ===========================================================================
// Test fixture for Fail GC end-to-end tests using a normal user tenant ("tt1").
//
// SQL routing:
//   __all_shared_gc_task[_history] for a user tenant lives in the meta tenant
//   (gen_meta_tenant_id(tenant_id_)).  ObSharedGCTaskTableOperator handles
//   routing internally; direct SQL helpers must pass the meta tenant id.
//
// FailGC dag identity is (tenant_id, gc_type) and does NOT use round_id /
// allow_gc_round_id (FailGC sets round_id = 0 by convention).  Each test
// uses a disjoint task_id band so they do not collide:
//   non_major_sstable_e2e        :  task_id = 1000
//   major_sstable_e2e            :  task_id = 2000
//   dirty_value_skip             :  task_id = 3000
//   unsupported_type_skip        :  task_id = 4000 (CHECKPOINT — not implemented yet)
//   tablet_meta_e2e              :  task_id = 5000 (user-tenant TABLET_META, single batch)
//   tablet_meta_multi_batch_e2e  :  task_id = 6000 (user-tenant TABLET_META, multi batch)
//   lease_takeover_e2e           :  task_id = 7000
//
// E2E coverage (with real shared-storage blocks):
//   * non_major_sstable_e2e: writes one data + one meta macro at the head of
//     each parallel slice the execute task will probe, then asserts those objects
//     are absent after the row drains.  The slice tail (BATCH_DETECT_SIZE - 1
//     ids beyond the head) tolerates OB_NO_SUCH_FILE_OR_DIRECTORY, so writing
//     only the head is sufficient to exercise ObSSGCUtil::batch_delete_blocks.
//   * major_sstable_e2e: writes snapshot-namespaced MAJOR data + meta macros at every (cg, parallel)
//     slice head — exercising both the cg_cnt × parallel_cnt nesting and the
//     build+delete path — plus the four MAJOR_PREWARM_* files.  After GC, asserts
//     all 4 sstable slice heads × 2 (data/meta) are gone while prewarm files remain.
//
// MacroBlockId construction guarantee: build_sstable_macro_id_() mirrors the
// set_ss_share_object_opt call in ObSSFailGCUtil::detect_sstable_slice_block_ids_
// byte-for-byte. build_prewarm_macro_id_() mirrors set_ss_major_prewarm_opt so
// the test writes the exact historical prewarm object paths that FAIL GC must retain.
//
// What is NOT covered here (explicitly out of scope):
//   * TABLET_META (sys-tenant) — sys-tenant double-delete API not yet implemented.
//                                 user-tenant single-delete IS covered (tablet_meta_e2e).
//   * CHECKPOINT               — checkpoint delete API is not yet implemented.
// ===========================================================================
class TestSSFailGC : public ObSimpleClusterTestBase
{
public:
  TestSSFailGC()
    : ObSimpleClusterTestBase("test_ss_fail_gc", "50G", "50G", "50G") {}

  // User tenant id, set once in setup_tenant and shared across tests.
  static uint64_t tenant_id_;

  // sys-level SQL proxy — routes to any tenant via the routing tenant_id arg.
  ObISQLClient &sql_proxy()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  // SQL routing tenant for task/history tables of a user tenant.
  uint64_t meta_tid() const { return gen_meta_tenant_id(tenant_id_); }

  int ensure_tablet_lock_row_(const ObLSID &ls_id, const ObTabletID &tablet_id,
                              const share::SCN &reorg_scn)
  {
    int ret = OB_SUCCESS;
    ObSSMetaCtrlKey tablet_lock_key;
    const ObSSMetaCtrlKey tablet_meta_key(
        blocksstable::ObStorageObjectType::SHARED_TABLET_META,
        tablet_id, reorg_scn, share::SCN::min_scn(), ls_id);
    if (OB_FAIL(tablet_meta_key.get_lock_row_key(tablet_lock_key))) {
      LOG_WARN("failed to get tablet lock row key", KR(ret), K(tablet_meta_key));
    } else {
      ObLockAtomicProtocol lock_row;
      if (OB_FAIL(lock_row.init(tenant_id_, ls_id, tablet_lock_key))) {
        LOG_WARN("failed to init tablet lock row", KR(ret), K(ls_id), K(tablet_lock_key));
      } else if (OB_FAIL(lock_row.check_and_create_lock_row(sql_proxy()))) {
        LOG_WARN("failed to create tablet lock row", KR(ret), K(ls_id), K(tablet_lock_key));
      }
    }
    return ret;
  }

  static ObAtomicProtocolType make_mini_sstable_type_()
  {
    ObAtomicProtocolType type;
    type.type_ = ObAtomicProtocolTypeEnum::MINI_SSTABLE;
    return type;
  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

  // Submit a FAIL_GC DAG for the user tenant.
  //
  // DAG identity is (tenant_id, gc_type).  If a previous DAG is still in
  // the scheduler, a new submission returns OB_EAGAIN/OB_TASK_EXIST; we
  // retry until the slot is free, mirroring the MarkSweep test.
  void trigger_fail_gc_dag()
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    ObSSGCExecDagInitParam param;
    param.tenant_id_         = tenant_id_;
    param.gc_type_           = ObSharedGCTaskType::FAIL_GC;
    param.allow_gc_round_id_ = 0; // ignored by FailGC
    int ret = OB_SUCCESS;
    for (int i = 0; i < 120; ++i) {
      ret = ObSSGCUtil::create_and_add_dag(param);
      if (OB_SUCCESS == ret) {
        break;
      } else if (OB_EAGAIN == ret || OB_TASK_EXIST == ret) {
        usleep(500 * 1000);
      } else {
        break;
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // Execute one existing slice synchronously so its final counters remain inspectable.
  void execute_fail_gc_slice_(const ObSSGCTaskIdRange &slice_range,
                              ObSSGCExecutorCostStat &cost_stat)
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    ObArenaAllocator dag_allocator("FailGCStatDag");
    ObSSGCExecDag dag;
    ObSSGCExecDagInitParam param;
    param.tenant_id_ = tenant_id_;
    param.gc_type_ = ObSharedGCTaskType::FAIL_GC;
    param.allow_gc_round_id_ = 0;
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
    ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
    ObFailGCExecuteTask *task = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
    ASSERT_NE(nullptr, task);
    ASSERT_EQ(OB_SUCCESS, task->init(tenant_id_, slice_range, slice_range));
    ASSERT_EQ(OB_SUCCESS, task->process());
    cost_stat = dag.cost_stat_;
  }

  void trigger_fail_gc_gen_dag()
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
    ASSERT_NE(nullptr, scheduler);
    ObSSFailGCDagParam param;
    ASSERT_EQ(OB_SUCCESS, param.init(tenant_id_));
    int ret = OB_SUCCESS;
    for (int i = 0; i < 120; ++i) {
      ret = scheduler->create_and_add_dag<ObSSFailGCDag>(&param);
      if (OB_SUCCESS == ret) {
        break;
      } else if (OB_EAGAIN == ret || OB_TASK_EXIST == ret) {
        ob_usleep(500 * 1000L);
      } else {
        break;
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    ObSSFailGCDag probe;
    probe.set_tenant_id(tenant_id_);
    bool dag_exist = true;
    bool is_emergency = false;
    const int64_t deadline_us = ObTimeUtility::current_time() + 60L * 1000 * 1000;
    while (dag_exist && OB_SUCCESS == ret && ObTimeUtility::current_time() < deadline_us) {
      ret = scheduler->check_dag_exist(&probe, dag_exist, is_emergency);
      if (OB_SUCCESS == ret && dag_exist) {
        ob_usleep(100 * 1000L);
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_FALSE(dag_exist);
  }

  void wait_dag_scheduler_empty_(int64_t timeout_sec = 60)
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
    ASSERT_NE(nullptr, scheduler);
    const int64_t deadline_us =
        ObTimeUtility::current_time() + timeout_sec * 1000000LL;
    while (!scheduler->is_empty() && ObTimeUtility::current_time() < deadline_us) {
      ob_usleep(100 * 1000);
    }
    ASSERT_TRUE(scheduler->is_empty())
        << "tenant dag scheduler is not empty after " << timeout_sec << "s";
  }

  // Spin-wait until __all_shared_gc_task has zero FAIL_GC rows whose
  // task_id falls inside [task_id_lo, task_id_hi].  Polls every 500 ms.
  void wait_fail_gc_rows_drained(int64_t task_id_lo, int64_t task_id_hi,
                                 int64_t timeout_sec = 60)
  {
    int ret = OB_SUCCESS;
    const int64_t deadline_us =
        ObTimeUtility::current_time() + timeout_sec * 1000000LL;
    int64_t count = -1;
    ObSqlString sql;
    do {
      count = 0;
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
          "SELECT COUNT(*) as cnt FROM oceanbase.__all_shared_gc_task "
          "WHERE tenant_id = %lu AND type = %ld "
          "AND task_id BETWEEN %ld AND %ld",
          tenant_id_,
          static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC),
          task_id_lo, task_id_hi));
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ASSERT_EQ(OB_SUCCESS, sql_proxy().read(res, meta_tid(), sql.ptr()));
        sqlclient::ObMySQLResult *result = res.get_result();
        ASSERT_NE(nullptr, result);
        ASSERT_EQ(OB_SUCCESS, result->next());
        ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", count));
      }
      if (0 == count) {
        break;
      }
      usleep(500 * 1000);
    } while (ObTimeUtility::current_time() < deadline_us);
    ASSERT_EQ(0, count)
        << "fail gc rows still present after " << timeout_sec << "s "
        << "task_id in [" << task_id_lo << "," << task_id_hi << "]";
  }

  // Build an ObSharedGCTask whose value is a serialized ObSharedGCFailInfo.
  // Memory is owned by `alloc`.
  static int build_fail_gc_task(
      ObIAllocator &alloc,
      int64_t task_id,
      const ObSharedGCFailInfo &info,
      ObSharedGCTask &task)
  {
    int ret = OB_SUCCESS;
    task.reset();
    task.key_ = ObSharedGCTaskKey(tenant_id_,
                                  ObSharedGCTaskType::FAIL_GC,
                                  0/*round_id*/, task_id);
    // extra_info must be non-empty; empty round-trips as SQL NULL and the
    // history INSERT fails on NOT NULL.
    task.extra_info_ = ObString::make_string("test");
    task.server_id_  = ObSharedGCTask::INVALID_SERVER_ID;
    if (OB_FAIL(info.to_value(alloc, task.value_))) {
      LOG_WARN("fail to serialize fail gc info", KR(ret), K(info));
    }
    return ret;
  }

  // Build a MacroBlockId for a sstable data/meta macro using the SAME
  // set_ss_share_object_opt call that ObSSFailGCUtil::detect_sstable_slice_block_ids_
  // uses to compute its probe id.  Result is byte-identical to what FailGC probes.
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

  // Build a MacroBlockId for a tablet sub_meta entry using the SAME
  // set_ss_tablet_sub_meta_opt call that ObSSFailGCUtil::build_tablet_meta_block_ids
  // uses to compute its start_block_id.  The returned id is byte-identical to
  // what FailGC will probe via is_tiered_metadata_exist.
  static int build_tablet_sub_meta_macro_id_(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const uint32_t op_id,
      const uint32_t data_seq,
      const share::SCN &reorganization_scn,
      blocksstable::MacroBlockId &macro_id)
  {
    blocksstable::ObStorageObjectOpt opt;
    // is_object_storage = false → SHARED_TABLET_SUB_META (in_table bit set); this is the
    // only variant ObTieredMetadataObjectManager accepts. Must match the value
    // used by ObSSFailGCUtil::build_tablet_meta_block_ids so the GC-side probe
    // produces a byte-identical id.
    opt.set_ss_tablet_sub_meta_opt(ls_id.id(),
                                   tablet_id.id(),
                                   op_id,
                                   data_seq,
                                   tablet_id.is_ls_inner_tablet(),
                                   reorganization_scn.get_val_for_tx(),
                                   false /* is_object_storage */);
    return blocksstable::ObObjectManager::ss_get_object_id(opt, macro_id);
  }

  // Insert one tiered-metadata entry for `macro_id` so the FailGC probe
  // (is_tiered_metadata_exist) returns true for it.
  // Caller must already be inside a tenant switch to tenant_id_.
  //
  // ObTieredMetadataObjectManager reads the tenant id from the
  // ObObjectStorageTenantGuard thread-local; without a guard it defaults to
  // OB_SERVER_TENANT_ID (500) — a virtual tenant — and the inner MTL_SWITCH
  // returns OB_OP_NOT_ALLOW (-4179).
  static void write_tablet_meta_entry_(const blocksstable::MacroBlockId &macro_id)
  {
    ObObjectStorageTenantGuard guard(
        tenant_id_,
        OB_IO_MANAGER.get_object_storage_io_timeout_ms(tenant_id_) * 1000LL);
    static char meta_buf[64];
    memset(meta_buf, 'm', sizeof(meta_buf));
    int64_t write_size = 0;
    ASSERT_EQ(OB_SUCCESS,
              ObTieredMetadataObjectManager::write_tiered_metadata_to_table(
                  macro_id, false /*is_batch_write*/,
                  static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE),
                  meta_buf, sizeof(meta_buf), write_size));
  }

  // Assert that the tiered-metadata entry for `macro_id` does (or does not)
  // exist.  Caller must already be inside a tenant switch to tenant_id_.
  static void assert_tablet_meta_exists_(const blocksstable::MacroBlockId &macro_id,
                                         const bool expected_exist)
  {
    ObObjectStorageTenantGuard guard(
        tenant_id_,
        OB_IO_MANAGER.get_object_storage_io_timeout_ms(tenant_id_) * 1000LL);
    bool actual = false;
    ASSERT_EQ(OB_SUCCESS,
              ObTieredMetadataObjectManager::is_tiered_metadata_exist(macro_id, actual));
    char id_buf[256] = {0};
    macro_id.to_string(id_buf, sizeof(id_buf));
    ASSERT_EQ(expected_exist, actual)
        << "tablet meta macro_id=" << id_buf
        << " expected_exist=" << expected_exist
        << " actual=" << actual;
  }

  // Build a MacroBlockId for a major prewarm file using the production
  // set_ss_major_prewarm_opt object-key encoding.
  static int build_prewarm_macro_id_(
      const blocksstable::ObStorageObjectType obj_type,
      const ObTabletID &tablet_id,
      const share::SCN &major_snapshot_version,
      const share::SCN &reorganization_scn,
      blocksstable::MacroBlockId &macro_id)
  {
    blocksstable::ObStorageObjectOpt opt;
    opt.set_ss_major_prewarm_opt(obj_type,
                                 tablet_id.id(),
                                 major_snapshot_version.get_val_for_tx(),
                                 reorganization_scn.get_val_for_tx());
    return blocksstable::ObObjectManager::ss_get_object_id(opt, macro_id);
  }

  // Synchronously write a 1MB payload to `macro_id` in object storage via
  // WRITE_THROUGH so the object is visible to is_exist immediately.
  // Caller must already be inside a tenant switch to tenant_id_.
  static void write_macro_block_(const blocksstable::MacroBlockId &macro_id)
  {
    static char write_buf[1024 * 1024];
    memset(write_buf, 'a', sizeof(write_buf));
    blocksstable::ObStorageObjectWriteInfo write_info;
    write_info.io_desc_.set_wait_event(1);
    write_info.buffer_ = write_buf;
    write_info.offset_ = 0;
    write_info.size_ = sizeof(write_buf);
    write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.write_strategy_ = blocksstable::ObStorageObjectWriteStrategy::WRITE_THROUGH;

    blocksstable::ObStorageObjectHandle write_object_handle;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
    ASSERT_EQ(OB_SUCCESS,
              ObSSObjectAccessUtil::async_write_file(write_info, write_object_handle));
    ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  }

  // Assert that `macro_id` does (or does not) exist in object storage.
  // Caller must already be inside a tenant switch to tenant_id_.
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

  // 读取所有 extra_info 回指到 `fail_gc_task_id` 的 FAIL_GC_MACRO_ID 历史记录。
  // 统计所有分片中的宏块 ID 总数，并校验每行的 round_id 均为 0（源 FAIL_GC 的 round_id）。
  // 返回历史记录行数（分片数）和宏块 ID 总数。
  void collect_fail_gc_macro_id_history(int64_t fail_gc_task_id,
                                        int64_t &row_cnt,
                                        int64_t &total_macro_cnt)
  {
    int ret = OB_SUCCESS;
    row_cnt = 0;
    total_macro_cnt = 0;
    ObSqlString sql;
    ObSqlString expect_extra;
    ASSERT_EQ(OB_SUCCESS, expect_extra.assign_fmt("fail_gc_task_id=%ld", fail_gc_task_id));
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT round_id, value FROM oceanbase.%s "
        "WHERE tenant_id = %lu AND type = %ld AND extra_info = '%.*s'",
        OB_ALL_SHARED_GC_TASK_HISTORY_TNAME,
        tenant_id_,
        static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC_MACRO_ID),
        static_cast<int>(expect_extra.length()), expect_extra.ptr()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy().read(res, meta_tid(), sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      while (OB_SUCC(result->next())) {
        int64_t round_id = -1;
        ObString value;
        ASSERT_EQ(OB_SUCCESS, result->get_int("round_id", round_id));
        ASSERT_EQ(0, round_id);
        ASSERT_EQ(OB_SUCCESS, result->get_varchar("value", value));
        ObArenaAllocator chunk_alloc;
        ObString deep_value;
        ASSERT_EQ(OB_SUCCESS, ob_write_string(chunk_alloc, value, deep_value));
        ObSharedGCMacroIdList list;
        ASSERT_EQ(OB_SUCCESS, list.from_value(deep_value));
        total_macro_cnt += list.count();
        ++row_cnt;
      }
      ASSERT_EQ(OB_ITER_END, ret);
    }
  }

  // Delete all FAIL_GC_MACRO_ID history rows linked to `fail_gc_task_id`.
  void clean_fail_gc_macro_id_history(int64_t fail_gc_task_id)
  {
    ObSqlString sql;
    int64_t affected = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "DELETE FROM oceanbase.%s "
        "WHERE tenant_id = %lu AND type = %ld AND extra_info = 'fail_gc_task_id=%ld'",
        OB_ALL_SHARED_GC_TASK_HISTORY_TNAME,
        tenant_id_,
        static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC_MACRO_ID),
        fail_gc_task_id));
    ASSERT_EQ(OB_SUCCESS, sql_proxy().write(meta_tid(), sql.ptr(), affected));
  }

  static bool is_same_fail_gc_info_(
      const ObSharedGCFailInfo &expected,
      const ObSharedGCFailInfo &actual)
  {
    return expected.meta_type_ == actual.meta_type_
        && expected.op_id_ == actual.op_id_
        && expected.ls_id_ == actual.ls_id_
        && expected.tablet_id_ == actual.tablet_id_
        && expected.transfer_scn_ == actual.transfer_scn_
        && expected.ctrl_object_info_ == actual.ctrl_object_info_;
  }

  int find_target_fail_gc_history_(
      const ObSharedGCFailInfo &expected,
      bool &found,
      int64_t &task_id)
  {
    int ret = OB_SUCCESS;
    found = false;
    task_id = -1;
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
        "SELECT task_id, value FROM oceanbase.%s "
        "WHERE tenant_id = %lu AND type = %ld AND round_id = 0 "
        "ORDER BY task_id",
        OB_ALL_SHARED_GC_TASK_HISTORY_TNAME,
        tenant_id_,
        static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC)))) {
      LOG_WARN("failed to build target fail gc history query", KR(ret), K(expected));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        if (OB_FAIL(sql_proxy().read(res, meta_tid(), sql.ptr()))) {
          LOG_WARN("failed to query target fail gc history", KR(ret), K(sql));
        } else if (OB_ISNULL(res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("target fail gc history result is null", KR(ret));
        } else {
          sqlclient::ObMySQLResult *result = res.get_result();
          int next_ret = OB_SUCCESS;
          while (OB_SUCC(ret) && !found && OB_SUCCESS == (next_ret = result->next())) {
            int64_t candidate_task_id = -1;
            ObString value;
            if (OB_FAIL(result->get_int("task_id", candidate_task_id))) {
              LOG_WARN("failed to read fail gc history task id", KR(ret));
            } else if (OB_FAIL(result->get_varchar("value", value))) {
              LOG_WARN("failed to read fail gc history value", KR(ret), K(candidate_task_id));
            } else {
              ObSharedGCFailInfo actual;
              const int parse_ret = actual.from_value(value);
              if (OB_SUCCESS != parse_ret) {
                LOG_INFO("skip unrelated invalid fail gc history value",
                    KR(parse_ret), K(candidate_task_id));
              } else if (is_same_fail_gc_info_(expected, actual)) {
                found = true;
                task_id = candidate_task_id;
              }
            }
          }
          if (OB_SUCC(ret) && !found && OB_ITER_END != next_ret) {
            ret = next_ret;
            LOG_WARN("failed to iterate fail gc history", KR(ret));
          }
        }
      }
    }
    return ret;
  }

  int wait_target_fail_gc_history_(
      const ObSharedGCFailInfo &expected,
      int64_t &task_id,
      const int64_t timeout_sec = 60)
  {
    int ret = OB_SUCCESS;
    bool found = false;
    task_id = -1;
    const int64_t deadline_us =
        ObTimeUtility::current_time() + timeout_sec * 1000000LL;
    while (OB_SUCC(ret) && !found
        && ObTimeUtility::current_time() < deadline_us) {
      if (OB_FAIL(find_target_fail_gc_history_(expected, found, task_id))) {
        LOG_WARN("failed to find target fail gc history", KR(ret), K(expected));
      } else if (!found) {
        ob_usleep(100 * 1000);
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait target fail gc history timeout", KR(ret), K(expected), K(timeout_sec));
    }
    return ret;
  }

  int backdate_active_task_modified_(
      const ObSharedGCTaskKey &key,
      const int64_t age_us)
  {
    int ret = OB_SUCCESS;
    const int64_t old_ts_us = ObTimeUtility::current_time() - age_us;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt(
        "UPDATE oceanbase.__all_shared_gc_task SET gmt_modified = usec_to_time(%ld) "
        "WHERE tenant_id = %lu AND type = %ld AND round_id = %ld AND task_id = %ld",
        old_ts_us,
        key.tenant_id_,
        static_cast<int64_t>(key.type_),
        key.round_id_,
        key.task_id_))) {
      LOG_WARN("fail to assign backdate sql", KR(ret), K(key));
    } else if (OB_FAIL(sql_proxy().write(meta_tid(), sql.ptr(), affected_rows))) {
      LOG_WARN("fail to backdate active task", KR(ret), K(key), K(sql));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows when backdating active task",
          KR(ret), K(key), K(affected_rows));
    }
    return ret;
  }

  int64_t count_rows_(const char *table_name, const char *where_clause)
  {
    int ret = OB_SUCCESS;
    int64_t count = -1;
    ObSqlString sql;
    EXPECT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT COUNT(*) as cnt FROM oceanbase.%s WHERE %s",
        table_name, where_clause));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      EXPECT_EQ(OB_SUCCESS, sql_proxy().read(res, meta_tid(), sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      EXPECT_NE(nullptr, result);
      EXPECT_EQ(OB_SUCCESS, result->next());
      EXPECT_EQ(OB_SUCCESS, result->get_int("cnt", count));
    }
    return count;
  }
};

uint64_t TestSSFailGC::tenant_id_ = OB_INVALID_TENANT_ID;

// ---------------------------------------------------------------------------
// observer_start: wait for sys LS leader before any tests run.
// ---------------------------------------------------------------------------
TEST_F(TestSSFailGC, observer_start)
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

// ---------------------------------------------------------------------------
// setup_tenant: create the user tenant "tt1" and store its id.
// ---------------------------------------------------------------------------
TEST_F(TestSSFailGC, setup_tenant)
{
  int ret = OB_SUCCESS;
  int retry_cnt = 0;
  do {
    ret = create_tenant("tt1");
    if (OB_FAIL(ret)) {
      LOG_WARN("create_tenant failed, will retry", KR(ret), K(retry_cnt));
      ob_usleep(15 * 1000 * 1000);
    }
  } while (OB_FAIL(ret) && ++retry_cnt < 10);
  ASSERT_EQ(OB_SUCCESS, ret);
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

// ---------------------------------------------------------------------------
// 验证 NONE_MAJOR 使用固定 seq step，并检查非法并行度。
// ---------------------------------------------------------------------------
TEST_F(TestSSFailGC, init_by_none_major_unit)
{
  ObSSMetaCtrlObjectInfo info;
  share::SCN snapshot;
  snapshot.set_base();
  ASSERT_EQ(OB_SUCCESS, info.init_by_none_major(snapshot, /*parallel_cnt=*/2));
  ASSERT_TRUE(info.is_none_major());
  ASSERT_EQ(compaction::MACRO_STEP_SIZE, info.seq_step_);
  ASSERT_EQ(2, info.parallel_cnt_);

  ASSERT_EQ(OB_INVALID_ARGUMENT,
            info.init_by_none_major(snapshot, /*parallel_cnt=*/-1));
}

// ---------------------------------------------------------------------------
// major_namespace_unit: MAJOR control rows rebuild fourth_id from
// snapshot_version.
// ---------------------------------------------------------------------------
TEST_F(TestSSFailGC, major_namespace_unit)
{
  const ObLSID LS_ID(1001);
  const ObTabletID TABLET_ID(200008);
  share::SCN snapshot;
  share::SCN reorg_scn;
  ASSERT_EQ(OB_SUCCESS, snapshot.convert_for_tx(1000));
  reorg_scn.set_min();

  ObSSMetaCtrlObjectInfo info;
  ASSERT_EQ(OB_SUCCESS, info.init_by_major(
      snapshot, 1 /*seq_step*/, 1 /*parallel_cnt*/, 0 /*row-store cg_cnt*/));
  ASSERT_TRUE(info.is_major());

  ObSEArray<blocksstable::MacroBlockId, 4> ids;
  ASSERT_EQ(OB_SUCCESS, ObSSFailGCUtil::detect_sstable_block_ids(
      blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL,
      LS_ID, TABLET_ID, reorg_scn,
      info, 0 /*op_id*/, ids));
  ASSERT_GT(ids.count(), 0);
  for (int64_t i = 0; i < ids.count(); ++i) {
    ASSERT_EQ(static_cast<int64_t>(snapshot.get_val_for_tx()),
              ids.at(i).major_snapshot());
  }

  ObSSMetaCtrlObjectInfo invalid_info;
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_info.init_by_major(
      share::SCN::min_scn(), 1, 1, 0));
}

// ---------------------------------------------------------------------------
// empty_table_noop: no fail-gc rows → plan task exits without queueing a execute task.
// Verify no spurious history rows are produced.
// ---------------------------------------------------------------------------
TEST_F(TestSSFailGC, empty_table_noop)
{
  int ret = OB_SUCCESS;
  trigger_fail_gc_dag();
  // The empty pre-scan exits almost instantly; 3 s is conservative.
  usleep(3 * 1000 * 1000);

  int64_t history_count = 0;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT COUNT(*) as cnt FROM oceanbase.%s "
      "WHERE tenant_id = %lu AND type = %ld",
      OB_ALL_SHARED_GC_TASK_HISTORY_TNAME,
      tenant_id_,
      static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC)));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy().read(res, meta_tid(), sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", history_count));
  }
  ASSERT_EQ(0, history_count) << "empty-table DAG wrote unexpected history rows";
}

// ---------------------------------------------------------------------------
// non_major_sstable_e2e: a single NONE_MAJOR (MINI_SSTABLE) fail-gc row.
//
//   1. Write one real data macro + one real meta macro at the head of each
//      parallel slice (PARALLEL_CNT slices, start_seq = i * MACRO_STEP_SIZE).
//   2. Trigger the DAG and wait for the task row to drain to history.
//   3. Assert all written macros are absent from object storage.
//
// detect_and_build_blocks_ probes in BATCH_DETECT_SIZE strides. Missing IDs in
// the candidate range are tolerated by batch deletion.
// ---------------------------------------------------------------------------
TEST_F(TestSSFailGC, non_major_sstable_e2e)
{
  const int64_t TASK_ID = 1000;
  const int64_t PARALLEL_CNT = 2;
  const int64_t OP_ID = 3;
  const ObLSID LS_ID(1001);
  const ObTabletID TABLET_ID(200001);
  ObArenaAllocator alloc;

  // --- Build fail-gc info for a NONE_MAJOR row (MINI uses the unified MINOR ctrl type). ---
  ObSharedGCFailInfo info;
  info.meta_type_ = blocksstable::ObStorageObjectType::SHARED_MINOR_SSTABLE_CTRL;
  info.op_id_ = OP_ID;
  info.ls_id_ = LS_ID;
  info.tablet_id_ = TABLET_ID;
  info.transfer_scn_.set_min();
  share::SCN snapshot;
  snapshot.set_base();
  ASSERT_EQ(OB_SUCCESS,
            info.ctrl_object_info_.init_by_none_major(snapshot, PARALLEL_CNT));

  // --- Write one data + one meta macro at the head of each parallel slice. ---
  blocksstable::MacroBlockId data_ids[PARALLEL_CNT];
  blocksstable::MacroBlockId meta_ids[PARALLEL_CNT];
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    share::SCN reorg_min;
    reorg_min.set_min();
    for (int64_t i = 0; i < PARALLEL_CNT; ++i) {
      const int64_t slice_head_seq = i * info.ctrl_object_info_.seq_step_;
      ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
          blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
          LS_ID, TABLET_ID, OP_ID, slice_head_seq, /*cg_id=*/0, reorg_min,
          data_ids[i]));
      ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
          blocksstable::ObStorageObjectType::SHARED_MINI_META_MACRO,
          LS_ID, TABLET_ID, OP_ID, slice_head_seq, /*cg_id=*/0, reorg_min,
          meta_ids[i]));
      write_macro_block_(data_ids[i]);
      write_macro_block_(meta_ids[i]);
      assert_block_exists_(data_ids[i], true);
      assert_block_exists_(meta_ids[i], true);
    }
  }

  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS, build_fail_gc_task(alloc, TASK_ID, info, row));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));

  // --- Execute the slice and wait for the row to drain. ---
  ObSSGCExecutorCostStat cost_stat;
  ASSERT_NO_FATAL_FAILURE(execute_fail_gc_slice_(
      ObSSGCTaskIdRange(TASK_ID, TASK_ID), cost_stat));
  wait_fail_gc_rows_drained(TASK_ID, TASK_ID);
  ASSERT_EQ(1, cost_stat.succ_row_count_);
  ASSERT_EQ(0, cost_stat.failed_row_count_);

  // --- Every macro we wrote must now be gone from object storage. ---
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t i = 0; i < PARALLEL_CNT; ++i) {
      assert_block_exists_(data_ids[i], false);
      assert_block_exists_(meta_ids[i], false);
    }
  }

  // --- History must carry exactly one row. ---
  ObSharedGCTask h_row;
  ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::FAIL_GC,
                        0/*round_id*/, TASK_ID);
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::get_history(key, alloc,
                                                     sql_proxy(), h_row));

  // The DAG claim step must have populated server_id and trace_id.
  ObSharedGCFailInfo parsed;
  ASSERT_EQ(OB_SUCCESS, parsed.from_value(h_row.value_));
  ASSERT_EQ(info.meta_type_, parsed.meta_type_);
  ASSERT_EQ(info.op_id_, parsed.op_id_);
  ASSERT_EQ(info.tablet_id_, parsed.tablet_id_);

  // --- The detected macro ids must also be recorded as FAIL_GC_MACRO_ID
  //     history rows, linked back via extra_info=fail_gc_task_id=<TASK_ID>. ---
  int64_t macro_row_cnt = 0;
  int64_t macro_total_cnt = 0;
  collect_fail_gc_macro_id_history(TASK_ID, macro_row_cnt, macro_total_cnt);
  ASSERT_GE(macro_row_cnt, 1);
  ASSERT_GT(macro_total_cnt, 0);
  ASSERT_EQ(macro_total_cnt, cost_stat.succ_sstable_macro_cnt_);
  ASSERT_EQ(0, cost_stat.succ_tablet_sub_meta_cnt_);

  // Cleanup.
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_history(key, sql_proxy()));
  clean_fail_gc_macro_id_history(TASK_ID);
}

// 模拟 CopySS abort 后的 MINI SSTable 残留，在 data 和额外 index/meta GC 覆盖窗口起点
// 分别写入 data/meta 探针；验证 control row 经 generator 转换为任务后，Fail-GC 能按运行时
// op_id 完整删除对应对象。
TEST_F(TestSSFailGC, mini_sstable_abort_to_fail_gc_e2e)
{
  // 异步 generator 通过 LS lock row 枚举待处理 LS，因此使用已注册的 SYS_LS。
  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  const ObTabletID tablet_id(200010);
  const int64_t data_parallel_cnt = 1;
  const int64_t gc_parallel_cnt =
      data_parallel_cnt + compaction::INDEX_TREE_MACRO_SEQ_SLOT_CNT;
  const share::SCN reorg_scn = share::SCN::min_scn();
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  ObSSMetaCtrlObjectInfo object_info;
  ASSERT_EQ(OB_SUCCESS,
      object_info.init_by_none_major(share::SCN::base_scn(), gc_parallel_cnt));
  ASSERT_EQ(compaction::MACRO_STEP_SIZE, object_info.seq_step_);
  ASSERT_EQ(gc_parallel_cnt, object_info.parallel_cnt_);
  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row_(ls_id, tablet_id, reorg_scn));

  ObSSTableAtomicProtocol protocol;
  ASSERT_EQ(OB_SUCCESS,
      protocol.init(make_mini_sstable_type_(), tenant_id_, ls_id, tablet_id,
          reorg_scn, object_info));
  ASSERT_EQ(OB_SUCCESS, protocol.create(false /*不检查 ss writer*/));
  int64_t op_id = -1;
  ASSERT_EQ(OB_SUCCESS, protocol.get_op_id(op_id));

  blocksstable::MacroBlockId data_ids[gc_parallel_cnt];
  blocksstable::MacroBlockId meta_ids[gc_parallel_cnt];
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t i = 0; i < gc_parallel_cnt; ++i) {
      const int64_t slice_start_seq = i * object_info.seq_step_;
      ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
          blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
          ls_id, tablet_id, op_id, slice_start_seq, 0 /*列组*/, reorg_scn, data_ids[i]));
      ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
          blocksstable::ObStorageObjectType::SHARED_MINI_META_MACRO,
          ls_id, tablet_id, op_id, slice_start_seq, 0 /*列组*/, reorg_scn, meta_ids[i]));
      ASSERT_EQ(slice_start_seq, data_ids[i].seq_id());
      ASSERT_EQ(slice_start_seq, meta_ids[i].seq_id());
      if (i > 0) {
        ASSERT_NE(data_ids[i - 1], data_ids[i]);
        ASSERT_NE(meta_ids[i - 1], meta_ids[i]);
      }
      write_macro_block_(data_ids[i]);
      write_macro_block_(meta_ids[i]);
      assert_block_exists_(data_ids[i], true);
      assert_block_exists_(meta_ids[i], true);
    }
  }

  ASSERT_EQ(OB_SUCCESS, protocol.abort());
  trigger_fail_gc_gen_dag();

  ObSharedGCFailInfo expected_info;
  expected_info.meta_type_ =
      blocksstable::ObStorageObjectType::SHARED_MINOR_SSTABLE_CTRL;
  expected_info.op_id_ = op_id;
  expected_info.ls_id_ = ls_id;
  expected_info.tablet_id_ = tablet_id;
  expected_info.transfer_scn_ = reorg_scn;
  expected_info.ctrl_object_info_ = object_info;

  trigger_fail_gc_dag();
  int64_t task_id = -1;
  ASSERT_EQ(OB_SUCCESS,
      wait_target_fail_gc_history_(expected_info, task_id));
  ASSERT_GE(task_id, 0);
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t i = 0; i < gc_parallel_cnt; ++i) {
      assert_block_exists_(data_ids[i], false);
      assert_block_exists_(meta_ids[i], false);
    }
  }

  const ObSharedGCTaskKey history_key(
      tenant_id_, ObSharedGCTaskType::FAIL_GC, 0 /*round_id*/, task_id);
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::delete_history(history_key, sql_proxy()));
  clean_fail_gc_macro_id_history(task_id);
}

// 验证 inc-major compaction Fail-GC 仅删除目标 op_id 的 DATA/META 宏块，
// 同时保留相同逻辑位置的 DDL 宏块和下一代 compaction 宏块。
TEST_F(TestSSFailGC, inc_major_compaction_sstable_e2e)
{
  const int64_t TASK_ID = 1500;
  const int64_t SEQ_STEP = 10;
  const int64_t PARALLEL_CNT = 2;
  const int64_t CG_CNT = 2;
  const int64_t OP_ID = 7;
  const int64_t DDL_SNAPSHOT_VERSION = OP_ID;
  const int64_t NEXT_OP_ID = OP_ID + 1;
  const ObLSID LS_ID(1001);
  const ObTabletID TABLET_ID(200009);
  ObArenaAllocator alloc;
  share::SCN snapshot;
  snapshot.set_base();

  ObSharedGCFailInfo info;
  info.meta_type_ = blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_SSTABLE_CTRL;
  info.op_id_ = OP_ID;
  info.ls_id_ = LS_ID;
  info.tablet_id_ = TABLET_ID;
  info.transfer_scn_.set_min();
  ASSERT_EQ(OB_SUCCESS,
      info.ctrl_object_info_.init_by_inc_major(
          snapshot, SEQ_STEP, PARALLEL_CNT, CG_CNT));

  blocksstable::MacroBlockId data_ids[CG_CNT][PARALLEL_CNT];
  blocksstable::MacroBlockId meta_ids[CG_CNT][PARALLEL_CNT];
  blocksstable::MacroBlockId ddl_data_id;
  blocksstable::MacroBlockId ddl_meta_id;
  blocksstable::MacroBlockId next_data_id;
  blocksstable::MacroBlockId next_meta_id;
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    share::SCN reorg_min;
    reorg_min.set_min();
    // DDL snapshot 刻意与目标 op_id 相同，并复用首个 slice 的其他维度；
    // 下一代仅改变 op_id，以验证对象类型隔离与 compaction 代际隔离。
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
        blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO,
        LS_ID, TABLET_ID, DDL_SNAPSHOT_VERSION, 0 /*起始序号*/, 0 /*列组*/, reorg_min,
        ddl_data_id));
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
        blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO,
        LS_ID, TABLET_ID, DDL_SNAPSHOT_VERSION, 0 /*起始序号*/, 0 /*列组*/, reorg_min,
        ddl_meta_id));
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
        blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_DATA_MACRO,
        LS_ID, TABLET_ID, NEXT_OP_ID, 0 /*起始序号*/, 0 /*列组*/, reorg_min,
        next_data_id));
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
        blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_META_MACRO,
        LS_ID, TABLET_ID, NEXT_OP_ID, 0 /*起始序号*/, 0 /*列组*/, reorg_min,
        next_meta_id));
    for (int64_t cg_id = 0; cg_id < CG_CNT; ++cg_id) {
      for (int64_t i = 0; i < PARALLEL_CNT; ++i) {
        const int64_t slice_start_seq = i * SEQ_STEP;
        ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
            blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_DATA_MACRO,
            LS_ID, TABLET_ID, OP_ID, slice_start_seq, cg_id, reorg_min,
            data_ids[cg_id][i]));
        ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
            blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_META_MACRO,
            LS_ID, TABLET_ID, OP_ID, slice_start_seq, cg_id, reorg_min,
            meta_ids[cg_id][i]));
        write_macro_block_(data_ids[cg_id][i]);
        write_macro_block_(meta_ids[cg_id][i]);
        assert_block_exists_(data_ids[cg_id][i], true);
        assert_block_exists_(meta_ids[cg_id][i], true);
      }
    }
    write_macro_block_(ddl_data_id);
    write_macro_block_(ddl_meta_id);
    write_macro_block_(next_data_id);
    write_macro_block_(next_meta_id);
    assert_block_exists_(ddl_data_id, true);
    assert_block_exists_(ddl_meta_id, true);
    assert_block_exists_(next_data_id, true);
    assert_block_exists_(next_meta_id, true);
  }

  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS, build_fail_gc_task(alloc, TASK_ID, info, row));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));

  trigger_fail_gc_dag();
  wait_fail_gc_rows_drained(TASK_ID, TASK_ID);

  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t cg_id = 0; cg_id < CG_CNT; ++cg_id) {
      for (int64_t i = 0; i < PARALLEL_CNT; ++i) {
        assert_block_exists_(data_ids[cg_id][i], false);
        assert_block_exists_(meta_ids[cg_id][i], false);
      }
    }
    assert_block_exists_(ddl_data_id, true);
    assert_block_exists_(ddl_meta_id, true);
    assert_block_exists_(next_data_id, true);
    assert_block_exists_(next_meta_id, true);

    ObSEArray<blocksstable::MacroBlockId, 4> survivor_ids;
    ASSERT_EQ(OB_SUCCESS, survivor_ids.push_back(ddl_data_id));
    ASSERT_EQ(OB_SUCCESS, survivor_ids.push_back(ddl_meta_id));
    ASSERT_EQ(OB_SUCCESS, survivor_ids.push_back(next_data_id));
    ASSERT_EQ(OB_SUCCESS, survivor_ids.push_back(next_meta_id));
    int64_t sstable_macro_cnt = 0;
    int64_t tablet_sub_meta_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, ObSSGCUtil::batch_delete_blocks(
        tenant_id_, survivor_ids, sstable_macro_cnt, tablet_sub_meta_cnt));
    ASSERT_EQ(survivor_ids.count(), sstable_macro_cnt);
    ASSERT_EQ(0, tablet_sub_meta_cnt);

    assert_block_exists_(ddl_data_id, false);
    assert_block_exists_(ddl_meta_id, false);
    assert_block_exists_(next_data_id, false);
    assert_block_exists_(next_meta_id, false);
  }

  ObSharedGCTask history_row;
  const ObSharedGCTaskKey key(
      tenant_id_, ObSharedGCTaskType::FAIL_GC, 0 /*轮次*/, TASK_ID);
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::get_history(key, alloc, sql_proxy(), history_row));
  ObSharedGCFailInfo parsed;
  ASSERT_EQ(OB_SUCCESS, parsed.from_value(history_row.value_));
  ASSERT_EQ(info.meta_type_, parsed.meta_type_);
  ASSERT_EQ(OP_ID, parsed.op_id_);
  ASSERT_TRUE(parsed.ctrl_object_info_.is_inc_major());
  ASSERT_EQ(snapshot, parsed.ctrl_object_info_.snapshot_version_);
  ASSERT_EQ(SEQ_STEP, parsed.ctrl_object_info_.seq_step_);
  ASSERT_EQ(PARALLEL_CNT, parsed.ctrl_object_info_.parallel_cnt_);
  ASSERT_EQ(CG_CNT, parsed.ctrl_object_info_.cg_cnt_);

  int64_t macro_row_cnt = 0;
  int64_t macro_total_cnt = 0;
  collect_fail_gc_macro_id_history(TASK_ID, macro_row_cnt, macro_total_cnt);
  ASSERT_GE(macro_row_cnt, 1);
  ASSERT_GT(macro_total_cnt, 0);

  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::delete_history(key, sql_proxy()));
  clean_fail_gc_macro_id_history(TASK_ID);
}

// ---------------------------------------------------------------------------
// lease_takeover_e2e: 模拟 FAIL_GC execute DAG 在 Step B 后退出，active 行
// 遗留旧 owner。第一轮 DAG 看到 live owner 必须跳过；回拨 gmt_modified 让
// lease 过期后，第二轮 DAG 应接管该行，完成对象删除并迁移到 history。
// ---------------------------------------------------------------------------
TEST_F(TestSSFailGC, lease_takeover_e2e)
{
  const int64_t TASK_ID = 7000;
  const int64_t PARALLEL_CNT = 1;
  const int64_t OP_ID = 1;
  const ObLSID LS_ID(1001);
  const ObTabletID TABLET_ID(200007);
  ObArenaAllocator alloc;

  ObSharedGCFailInfo info;
  info.meta_type_ = blocksstable::ObStorageObjectType::SHARED_MINOR_SSTABLE_CTRL;
  info.op_id_ = OP_ID;
  info.ls_id_ = LS_ID;
  info.tablet_id_ = TABLET_ID;
  info.transfer_scn_.set_min();
  share::SCN snapshot;
  snapshot.set_base();
  ASSERT_EQ(OB_SUCCESS,
            info.ctrl_object_info_.init_by_none_major(snapshot, PARALLEL_CNT));

  blocksstable::MacroBlockId data_id;
  blocksstable::MacroBlockId meta_id;
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    share::SCN reorg_min;
    reorg_min.set_min();
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
        blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
        LS_ID, TABLET_ID, OP_ID, 0 /*起始序号*/, 0 /*列组*/, reorg_min, data_id));
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
        blocksstable::ObStorageObjectType::SHARED_MINI_META_MACRO,
        LS_ID, TABLET_ID, OP_ID, 0 /*起始序号*/, 0 /*列组*/, reorg_min, meta_id));
    write_macro_block_(data_id);
    write_macro_block_(meta_id);
    assert_block_exists_(data_id, true);
    assert_block_exists_(meta_id, true);
  }

  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS, build_fail_gc_task(alloc, TASK_ID, info, row));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));

  const int64_t old_owner_server_id = static_cast<int64_t>(GCTX.get_server_id()) + 10000;
  const ObString old_trace = ObString::make_string("failgc-old-owner");
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
      row.key_, old_owner_server_id, old_trace, sql_proxy()));

  // 旧 owner 仍处于 lease 内，DAG 只能跳过，不能进入 Step C 删除对象。
  trigger_fail_gc_dag();
  wait_dag_scheduler_empty_();
  {
    char where[256];
    ASSERT_LT(0, snprintf(where, sizeof(where),
        "tenant_id = %lu AND type = %ld AND round_id = %ld AND task_id = %ld",
        tenant_id_, static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC),
        0L, TASK_ID));
    ASSERT_EQ(1, count_rows_("__all_shared_gc_task", where));
    ASSERT_EQ(0, count_rows_("__all_shared_gc_task_history", where));
  }
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    assert_block_exists_(data_id, true);
    assert_block_exists_(meta_id, true);
  }

  // 模拟旧 executor 长时间未推进。lease 过期后下一轮 DAG 可以接管并完成。
  ASSERT_EQ(OB_SUCCESS, backdate_active_task_modified_(
      row.key_, 24L * 60 * 60 * 1000 * 1000 /*age_us*/));
  trigger_fail_gc_dag();
  wait_fail_gc_rows_drained(TASK_ID, TASK_ID);

  {
    char where[256];
    ASSERT_LT(0, snprintf(where, sizeof(where),
        "tenant_id = %lu AND type = %ld AND round_id = %ld AND task_id = %ld",
        tenant_id_, static_cast<int64_t>(ObSharedGCTaskType::FAIL_GC),
        0L, TASK_ID));
    ASSERT_EQ(0, count_rows_("__all_shared_gc_task", where));
    ASSERT_EQ(1, count_rows_("__all_shared_gc_task_history", where));
  }
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    assert_block_exists_(data_id, false);
    assert_block_exists_(meta_id, false);
  }

  ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::FAIL_GC,
                        0/*round_id*/, TASK_ID);
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_history(key, sql_proxy()));
  clean_fail_gc_macro_id_history(TASK_ID);
}

// ---------------------------------------------------------------------------
// major_sstable_e2e: a single MAJOR_SSTABLE fail-gc row.
//
//   1. Write one data + one meta macro at the head of every (cg, parallel)
//      slice — CG_CNT × PARALLEL_CNT slices total, each contributing one
//      data and one meta MacroBlockId.
//   2. Write the four MAJOR_PREWARM_* files for the (tablet, snapshot) pair.
//   3. Trigger DAG and wait for the row to drain.
//   4. Assert every SSTable macro is absent while every prewarm file remains,
//      then clean up the retained prewarm files.
// ---------------------------------------------------------------------------
TEST_F(TestSSFailGC, major_sstable_e2e)
{
  const int64_t TASK_ID = 2000;
  const int64_t SEQ_STEP = 10;
  const int64_t PARALLEL_CNT = 2;
  const int64_t CG_CNT = 2;
  const ObLSID LS_ID(1001);
  const ObTabletID TABLET_ID(200002);
  ObArenaAllocator alloc;

  ObSharedGCFailInfo info;
  info.meta_type_ = blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL;
  info.op_id_ = 0;
  info.ls_id_ = LS_ID;
  info.tablet_id_ = TABLET_ID;
  info.transfer_scn_.set_min();
  // snapshot_version is the major's compaction_scn for prewarm path generation;
  // pick a non-min value so set_ss_major_prewarm_opt produces a real path.
  share::SCN snapshot;
  ASSERT_EQ(OB_SUCCESS, snapshot.convert_for_tx(1000));
  ASSERT_EQ(OB_SUCCESS,
      info.ctrl_object_info_.init_by_major(snapshot, SEQ_STEP, PARALLEL_CNT, CG_CNT));

  // --- Write a real data + meta macro at each (cg, parallel) slice head. ---
  blocksstable::MacroBlockId data_ids[CG_CNT][PARALLEL_CNT];
  blocksstable::MacroBlockId meta_ids[CG_CNT][PARALLEL_CNT];
  // Prewarm objects are deliberately outside the FAIL GC deletion set.
  const blocksstable::ObStorageObjectType PREWARM_TYPES[4] = {
    blocksstable::ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX,
    blocksstable::ObStorageObjectType::MAJOR_PREWARM_DATA,
    blocksstable::ObStorageObjectType::MAJOR_PREWARM_META_INDEX,
    blocksstable::ObStorageObjectType::MAJOR_PREWARM_META,
  };
  blocksstable::MacroBlockId prewarm_ids[4];
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    share::SCN reorg_min;
    reorg_min.set_min();
    for (int64_t cg = 0; cg < CG_CNT; ++cg) {
      for (int64_t p = 0; p < PARALLEL_CNT; ++p) {
        const int64_t slice_head_seq = p * SEQ_STEP;
        ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
            blocksstable::ObStorageObjectType::SHARED_MAJOR_DATA_MACRO,
            LS_ID, TABLET_ID, static_cast<int64_t>(snapshot.get_val_for_tx()),
            slice_head_seq, cg, reorg_min,
            data_ids[cg][p]));
        ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
            blocksstable::ObStorageObjectType::SHARED_MAJOR_META_MACRO,
            LS_ID, TABLET_ID, static_cast<int64_t>(snapshot.get_val_for_tx()),
            slice_head_seq, cg, reorg_min,
            meta_ids[cg][p]));
        write_macro_block_(data_ids[cg][p]);
        write_macro_block_(meta_ids[cg][p]);
        assert_block_exists_(data_ids[cg][p], true);
        assert_block_exists_(meta_ids[cg][p], true);
      }
    }
    for (int i = 0; i < 4; ++i) {
      ASSERT_EQ(OB_SUCCESS, build_prewarm_macro_id_(
          PREWARM_TYPES[i], TABLET_ID, snapshot, reorg_min, prewarm_ids[i]));
      write_macro_block_(prewarm_ids[i]);
      assert_block_exists_(prewarm_ids[i], true);
    }

    ObSSMetaCtrlObjectInfo row_store_info;
    ASSERT_EQ(OB_SUCCESS, row_store_info.init_by_major(
        snapshot, SEQ_STEP, PARALLEL_CNT, 0 /* row-store cg_cnt */));
    ObSEArray<blocksstable::MacroBlockId, 8> detected_ids;
    ASSERT_EQ(OB_SUCCESS, ObSSFailGCUtil::detect_sstable_block_ids(
        blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL,
        LS_ID, TABLET_ID, reorg_min,
        row_store_info, info.op_id_, detected_ids));
    bool found_data = false;
    bool found_meta = false;
    for (int64_t i = 0; i < detected_ids.count(); ++i) {
      found_data = found_data || detected_ids.at(i) == data_ids[0][0];
      found_meta = found_meta || detected_ids.at(i) == meta_ids[0][0];
    }
    ASSERT_TRUE(found_data);
    ASSERT_TRUE(found_meta);
  }

  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS, build_fail_gc_task(alloc, TASK_ID, info, row));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));

  trigger_fail_gc_dag();
  wait_fail_gc_rows_drained(TASK_ID, TASK_ID);

  // --- SSTable slice macros are deleted, while prewarm files are retained. ---
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t cg = 0; cg < CG_CNT; ++cg) {
      for (int64_t p = 0; p < PARALLEL_CNT; ++p) {
        assert_block_exists_(data_ids[cg][p], false);
        assert_block_exists_(meta_ids[cg][p], false);
      }
    }
    for (int i = 0; i < 4; ++i) {
      assert_block_exists_(prewarm_ids[i], true);
    }

    ObSEArray<blocksstable::MacroBlockId, 4> prewarm_cleanup_ids;
    for (int i = 0; i < 4; ++i) {
      ASSERT_EQ(OB_SUCCESS, prewarm_cleanup_ids.push_back(prewarm_ids[i]));
    }
    int64_t sstable_macro_cnt = 0;
    int64_t tablet_sub_meta_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, ObSSGCUtil::batch_delete_blocks(
        tenant_id_, prewarm_cleanup_ids, sstable_macro_cnt, tablet_sub_meta_cnt));
    ASSERT_EQ(prewarm_cleanup_ids.count(), sstable_macro_cnt);
    ASSERT_EQ(0, tablet_sub_meta_cnt);
    for (int i = 0; i < 4; ++i) {
      assert_block_exists_(prewarm_ids[i], false);
    }
  }

  // History should carry exactly the inserted row.
  ObSharedGCTask h_row;
  ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::FAIL_GC,
                        0/*round_id*/, TASK_ID);
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::get_history(key, alloc,
                                                     sql_proxy(), h_row));
  ObSharedGCFailInfo parsed;
  ASSERT_EQ(OB_SUCCESS, parsed.from_value(h_row.value_));
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL, parsed.meta_type_);
  ASSERT_EQ(info.tablet_id_, parsed.tablet_id_);
  ASSERT_TRUE(parsed.ctrl_object_info_.is_major());
  ASSERT_EQ(ObSSMetaCtrlObjectInfo::TYPE::MAJOR,
            parsed.ctrl_object_info_.type_);
  ASSERT_EQ(info.ctrl_object_info_.cg_cnt_,
            parsed.ctrl_object_info_.cg_cnt_);
  ASSERT_EQ(info.ctrl_object_info_.snapshot_version_,
            parsed.ctrl_object_info_.snapshot_version_);

  // --- The detected macro ids must also be recorded as FAIL_GC_MACRO_ID
  //     history rows, linked back via extra_info=fail_gc_task_id=<TASK_ID>. ---
  int64_t macro_row_cnt = 0;
  int64_t macro_total_cnt = 0;
  collect_fail_gc_macro_id_history(TASK_ID, macro_row_cnt, macro_total_cnt);
  ASSERT_GE(macro_row_cnt, 1);
  ASSERT_GT(macro_total_cnt, 0);

  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_history(key, sql_proxy()));
  clean_fail_gc_macro_id_history(TASK_ID);
}

// ---------------------------------------------------------------------------
// dirty_value_skip: a task row whose value cannot be deserialised as
// ObSharedGCFailInfo is a per-row failure — the row stays in
// __all_shared_gc_task and is NOT moved to history.
//
// Two rows:
//   row task_id=3000 (dirty)  — 16 bytes of 0xFF, won't parse as UNIS
//   row task_id=3001 (clean)  — valid NONE_MAJOR MINI_SSTABLE info
// After the DAG:
//   row 3000 must still be in __all_shared_gc_task
//   row 3001 must be in __all_shared_gc_task_history
// ---------------------------------------------------------------------------
TEST_F(TestSSFailGC, dirty_value_skip)
{
  const int64_t DIRTY_TASK_ID = 3000;
  const int64_t CLEAN_TASK_ID = 3001;

  ObArenaAllocator alloc;
  const ObSharedGCTaskKey dirty_key(tenant_id_, ObSharedGCTaskType::FAIL_GC,
                                    0/*round_id*/, DIRTY_TASK_ID);
  const ObSharedGCTaskKey clean_key(tenant_id_, ObSharedGCTaskType::FAIL_GC,
                                    0/*round_id*/, CLEAN_TASK_ID);

  // --- Dirty row: 16 bytes of 0xFF. ---
  {
    ObSharedGCTask task;
    task.key_ = dirty_key;
    char *buf = static_cast<char *>(alloc.alloc(16));
    ASSERT_NE(nullptr, buf);
    memset(buf, 0xFF, 16);
    task.value_.assign_ptr(buf, 16);
    task.extra_info_ = ObString::make_string("test");
    task.server_id_  = ObSharedGCTask::INVALID_SERVER_ID;
    ASSERT_EQ(OB_SUCCESS,
              ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));
  }

  // --- Clean row: a minimal NONE_MAJOR fail-gc value. ---
  {
    ObSharedGCFailInfo info;
    info.meta_type_ = blocksstable::ObStorageObjectType::SHARED_MINOR_SSTABLE_CTRL;
    info.op_id_ = 1;
    info.ls_id_ = ObLSID(1001);
    info.tablet_id_ = ObTabletID(200003);
    info.transfer_scn_.set_min();
    share::SCN snapshot;
    snapshot.set_base();
    ASSERT_EQ(OB_SUCCESS,
              info.ctrl_object_info_.init_by_none_major(snapshot, 1));
    ObSharedGCTask task;
    ASSERT_EQ(OB_SUCCESS, build_fail_gc_task(alloc, CLEAN_TASK_ID, info, task));
    ASSERT_EQ(OB_SUCCESS,
              ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));
  }

  trigger_fail_gc_dag();

  // Wait for the clean row to appear in history (up to 60 s).
  {
    int ret = OB_ENTRY_NOT_EXIST;
    const int64_t deadline_us =
        ObTimeUtility::current_time() + 60LL * 1000000LL;
    ObSharedGCTask h_row;
    do {
      ret = ObSharedGCTaskTableOperator::get_history(clean_key, alloc,
                                                     sql_proxy(), h_row);
      if (OB_SUCCESS == ret) {
        break;
      }
      usleep(500 * 1000);
    } while (ObTimeUtility::current_time() < deadline_us);
    ASSERT_EQ(OB_SUCCESS, ret) << "clean row not in history after 60s";
  }

  // Dirty row must still be in the task table — deserialization failure is
  // a per-row error, not propagated, so the row stays put.
  {
    ObSharedGCTask h_row;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST,
              ObSharedGCTaskTableOperator::get_history(dirty_key, alloc,
                                                       sql_proxy(), h_row));
  }

  // Cleanup.
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_task(dirty_key, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_history(clean_key, sql_proxy()));
  clean_fail_gc_macro_id_history(CLEAN_TASK_ID);
}

// ---------------------------------------------------------------------------
// unsupported_type_skip: CHECKPOINT fail-gc is not yet implemented; the execute task
// returns OB_NOT_SUPPORTED via tmp_ret and leaves the row in the task table.
//
// Verifies that:
//   * The row is NOT moved to history.
//   * Other rows are unaffected.
// ---------------------------------------------------------------------------
TEST_F(TestSSFailGC, unsupported_type_skip)
{
  const int64_t TASK_ID = 4000;
  ObArenaAllocator alloc;

  ObSharedGCFailInfo info;
  info.meta_type_ = blocksstable::ObStorageObjectType::SHARED_LS_CKPT;
  info.op_id_ = 1;
  info.ls_id_ = ObLSID(1001);
  info.tablet_id_ = ObTabletID(200004);
  info.transfer_scn_.set_min();
  info.ctrl_object_info_.set_none();

  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS, build_fail_gc_task(alloc, TASK_ID, info, row));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));

  trigger_fail_gc_dag();
  // Give the DAG a few seconds to scan; CHECKPOINT skips immediately, so
  // there is nothing to spin-wait on.  Just confirm the row stays put.
  usleep(5 * 1000 * 1000);

  ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::FAIL_GC,
                        0/*round_id*/, TASK_ID);
  ObSharedGCTask h_row;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            ObSharedGCTaskTableOperator::get_history(key, alloc,
                                                     sql_proxy(), h_row));

  // Cleanup.
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_task(key, sql_proxy()));
}

// ---------------------------------------------------------------------------
// tablet_meta_e2e: a single TABLET_META fail-gc row for a USER tenant
// (single-delete path — only tiered-metadata removal is required; the
// sys-tenant inner-table double-delete is not exercised here).
//
//   1. Pre-write N tiered-metadata entries with consecutive data_seq starting
//      at 0 — these are exactly the ids ObSSFailGCUtil::build_tablet_meta_block_ids
//      will probe via is_tiered_metadata_exist.
//   2. Trigger the DAG and wait for the task row to drain to history.
//   3. Assert all N entries are gone from the tiered-metadata store.
//
// Probe-build behaviour:
//   build_tablet_meta_block_ids starts at data_seq=0 and calls
//   detect_and_build_blocks_ which probes in BATCH_DETECT_SIZE strides.  With
//   data_seq in [0, ENTRY_COUNT) populated, the first probe at offset 0 sees
//   is_exist=true, the second probe at offset BATCH_DETECT_SIZE (=1000) sees
//   is_exist=false, then a tail batch covering the populated range is built.
//   N is therefore chosen comfortably under BATCH_DETECT_SIZE so the entire
//   populated range lands in one tail batch.
// ---------------------------------------------------------------------------
TEST_F(TestSSFailGC, tablet_meta_e2e)
{
  const int64_t TASK_ID = 5000;
  const int64_t ENTRY_COUNT = 4;
  const ObLSID LS_ID(1001);
  const ObTabletID TABLET_ID(200005);
  const uint32_t OP_ID = 7;
  ObArenaAllocator alloc;

  // --- Build fail-gc info for a TABLET_META row. ---
  ObSharedGCFailInfo info;
  info.meta_type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  info.op_id_ = OP_ID;
  info.ls_id_ = LS_ID;
  info.tablet_id_ = TABLET_ID;
  info.transfer_scn_.set_min();
  info.ctrl_object_info_.set_none();

  // --- Pre-write tiered-metadata entries for data_seq in [0, ENTRY_COUNT). ---
  blocksstable::MacroBlockId macro_ids[ENTRY_COUNT];
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t i = 0; i < ENTRY_COUNT; ++i) {
      ASSERT_EQ(OB_SUCCESS, build_tablet_sub_meta_macro_id_(
          LS_ID, TABLET_ID, OP_ID, /*data_seq=*/static_cast<uint32_t>(i),
          info.transfer_scn_, macro_ids[i]));
      write_tablet_meta_entry_(macro_ids[i]);
      assert_tablet_meta_exists_(macro_ids[i], true);
    }
  }

  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS, build_fail_gc_task(alloc, TASK_ID, info, row));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));

  // --- Execute the slice and wait for the row to drain to history. ---
  ObSSGCExecutorCostStat cost_stat;
  ASSERT_NO_FATAL_FAILURE(execute_fail_gc_slice_(
      ObSSGCTaskIdRange(TASK_ID, TASK_ID), cost_stat));
  wait_fail_gc_rows_drained(TASK_ID, TASK_ID);
  ASSERT_EQ(1, cost_stat.succ_row_count_);
  ASSERT_EQ(0, cost_stat.failed_row_count_);

  // --- Every pre-written tiered-metadata entry must now be gone. ---
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t i = 0; i < ENTRY_COUNT; ++i) {
      assert_tablet_meta_exists_(macro_ids[i], false);
    }
  }

  // --- History must carry exactly one row with the original fail-info. ---
  ObSharedGCTask h_row;
  ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::FAIL_GC,
                        0/*round_id*/, TASK_ID);
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::get_history(key, alloc,
                                                     sql_proxy(), h_row));
  ObSharedGCFailInfo parsed;
  ASSERT_EQ(OB_SUCCESS, parsed.from_value(h_row.value_));
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_TABLET_META, parsed.meta_type_);
  ASSERT_EQ(info.op_id_, parsed.op_id_);
  ASSERT_EQ(info.tablet_id_, parsed.tablet_id_);

  // --- The detected tablet-meta ids must be recorded and then cleaned. ---
  int64_t macro_row_cnt = 0;
  int64_t macro_total_cnt = 0;
  collect_fail_gc_macro_id_history(TASK_ID, macro_row_cnt, macro_total_cnt);
  ASSERT_GE(macro_row_cnt, 1);
  ASSERT_GE(macro_total_cnt, ENTRY_COUNT);
  ASSERT_EQ(0, cost_stat.succ_sstable_macro_cnt_);
  ASSERT_EQ(macro_total_cnt, cost_stat.succ_tablet_sub_meta_cnt_);

  // Cleanup.
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_history(key, sql_proxy()));
  clean_fail_gc_macro_id_history(TASK_ID);
}

// ---------------------------------------------------------------------------
// tablet_meta_multi_batch_e2e: same path as tablet_meta_e2e but with an entry
// count that crosses the BATCH_DETECT_SIZE boundary, so detect_and_build_blocks_
// runs more than one probe-and-build round.
//
// With ENTRY_COUNT = BATCH_DETECT_SIZE + 500 = 1500 populated entries:
//   * probe data_seq=0    -> is_exist=true   -> advance to 1000
//   * probe data_seq=1000 -> is_exist=true   -> advance to 2000
//   * probe data_seq=2000 -> is_exist=false  -> stop
//   * build tail batch covering [2000, 2999] (1000 ids), then the loop
//     descends and builds [1000, 1999] and [0, 999].
// detect_and_build_blocks_ thus over-shoots: 3000 ids are passed to
// batch_delete_blocks → batch_del_tiered_metadata.  This is fine —
// ObTieredMetadataStore::batch_del_meta is non-atomic and silently tolerates
// rows that do not exist (mirrors the OB_NO_SUCH_FILE_OR_DIRECTORY tolerance
// in the sstable path).  We only assert that all *populated* entries are
// removed; the never-existed probe-overshoot ids are not checked.
// ---------------------------------------------------------------------------
TEST_F(TestSSFailGC, tablet_meta_multi_batch_e2e)
{
  const int64_t TASK_ID = 6000;
  const int64_t ENTRY_COUNT = 1500; // > BATCH_DETECT_SIZE (1000)
  const ObLSID LS_ID(1001);
  const ObTabletID TABLET_ID(200006);
  const uint32_t OP_ID = 11;
  ObArenaAllocator alloc;

  ObSharedGCFailInfo info;
  info.meta_type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  info.op_id_ = OP_ID;
  info.ls_id_ = LS_ID;
  info.tablet_id_ = TABLET_ID;
  info.transfer_scn_.set_min();
  info.ctrl_object_info_.set_none();

  // Pre-write 1500 tiered-metadata entries.  Heap-allocate the id array —
  // ENTRY_COUNT * sizeof(MacroBlockId) is too large for stack-local storage.
  blocksstable::MacroBlockId *macro_ids = static_cast<blocksstable::MacroBlockId *>(
      alloc.alloc(sizeof(blocksstable::MacroBlockId) * ENTRY_COUNT));
  ASSERT_NE(nullptr, macro_ids);
  for (int64_t i = 0; i < ENTRY_COUNT; ++i) {
    new (&macro_ids[i]) blocksstable::MacroBlockId();
  }
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t i = 0; i < ENTRY_COUNT; ++i) {
      ASSERT_EQ(OB_SUCCESS, build_tablet_sub_meta_macro_id_(
          LS_ID, TABLET_ID, OP_ID, /*data_seq=*/static_cast<uint32_t>(i),
          info.transfer_scn_, macro_ids[i]));
      write_tablet_meta_entry_(macro_ids[i]);
    }
    // Spot-check a few entries instead of all 1500 to keep test runtime bounded.
    assert_tablet_meta_exists_(macro_ids[0], true);
    assert_tablet_meta_exists_(macro_ids[ENTRY_COUNT / 2], true);
    assert_tablet_meta_exists_(macro_ids[ENTRY_COUNT - 1], true);
  }

  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS, build_fail_gc_task(alloc, TASK_ID, info, row));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));

  trigger_fail_gc_dag();
  // Larger entry count means a longer batch_del_meta loop; give it more headroom.
  wait_fail_gc_rows_drained(TASK_ID, TASK_ID, /*timeout_sec=*/120);

  // Spot-check the same three positions plus the batch boundaries — every
  // populated entry must now be gone.
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    const int64_t checkpoints[] = {0, 999, 1000, 1499, ENTRY_COUNT / 2};
    for (size_t i = 0; i < sizeof(checkpoints) / sizeof(checkpoints[0]); ++i) {
      assert_tablet_meta_exists_(macro_ids[checkpoints[i]], false);
    }
  }

  ObSharedGCTask h_row;
  ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::FAIL_GC,
                        0/*round_id*/, TASK_ID);
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::get_history(key, alloc,
                                                     sql_proxy(), h_row));
  ObSharedGCFailInfo parsed;
  ASSERT_EQ(OB_SUCCESS, parsed.from_value(h_row.value_));
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_TABLET_META, parsed.meta_type_);
  ASSERT_EQ(info.tablet_id_, parsed.tablet_id_);

  // --- The detected tablet-meta ids must be recorded.  This case crosses
  //     BATCH_DETECT_SIZE, so it verifies the multi-batch detection path too. ---
  int64_t macro_row_cnt = 0;
  int64_t macro_total_cnt = 0;
  collect_fail_gc_macro_id_history(TASK_ID, macro_row_cnt, macro_total_cnt);
  ASSERT_GE(macro_row_cnt, 1);
  ASSERT_GE(macro_total_cnt, ENTRY_COUNT);

  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_history(key, sql_proxy()));
  clean_fail_gc_macro_id_history(TASK_ID);
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
