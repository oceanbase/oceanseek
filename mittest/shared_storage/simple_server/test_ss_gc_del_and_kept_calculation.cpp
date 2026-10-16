/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#include <gtest/gtest.h>

#define protected public
#define private public
#include "src/storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"
#include "lib/utility/ob_test_util.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/incremental/atomic_protocol/ob_lock_atomic_protocol.h"
#include "src/storage/incremental/ob_sswriter_service.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/ob_cluster_version.h"
#include "logservice/ob_garbage_collector.h"
#include "src/storage/ss_garbage_collector/ob_ss_gc_define.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace sslog
{

} // namespace sslog
} // namespace oceanbase


using namespace oceanbase::transaction;
using namespace oceanbase::storage;

static int64_t lease_epoch = 1;

int ObSSWriterService::check_lease(
    const ObSSWriterKey &key,
    bool &is_sswriter,
    int64_t &epoch)
{
  is_sswriter = true;
  epoch = ATOMIC_LOAD(&lease_epoch);
  return OB_SUCCESS;
}

int ObSSWriterService::get_sswriter_addr(
    const ObSSWriterKey &key,
    ObSSWriterAddr &sswriter_addr,
    const int64_t timeout_us)
{
  sswriter_addr.addr_ = GCTX.self_addr();
  return OB_SUCCESS;
}

namespace oceanbase
{
char *shared_storage_info = nullptr;
bool new_tenant_created = false;
namespace unittest
{

struct TestRunCtx
{
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;

  TO_STRING_KV(K(tenant_id_), K(tenant_epoch_), K(ls_id_), K(ls_epoch_), K(tablet_id_));
};

// Fixture for mark-sweep GC del/kept-meta-scn calculation tests.  Reuses the
// shared-storage simple-server bootstrap and the create_tablet flow from
// test_shared_meta_service.cpp.
class TestSSGCDelAndKeptCalculation : public ObSimpleClusterTestBase
{
public:
  TestSSGCDelAndKeptCalculation()
    : ObSimpleClusterTestBase("test_ss_gc_del_and_kept_calculation", "50G", "50G", "50G")
  {}

  int exe_sql(const char *sql_str)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_ISNULL(sql_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql str is null", KR(ret), KP(sql_str));
    } else if (OB_FAIL(sql.assign(sql_str))) {
      LOG_WARN("fail to assign sql", KR(ret), K(sql_str));
    } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql_str), K(sql));
    }
    return ret;
  }

  int sys_exe_sql(const char *sql_str)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_ISNULL(sql_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql str is null", KR(ret), KP(sql_str));
    } else if (OB_FAIL(sql.assign(sql_str))) {
      LOG_WARN("fail to assign sql", KR(ret), K(sql_str));
    } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy().write(sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql_str), K(sql));
    }
    return ret;
  }

  virtual void SetUp() override
  {
    int ret = OB_SUCCESS;
    ObSimpleClusterTestBase::SetUp();
    oceanbase::palf::election::MAX_TST = 500 * 1000;  // 500ms, lease interval = 4 * 500ms = 2s
    if (!new_tenant_created) {
      LOG_INFO("start to create tt1 tenant");
      OK(create_tenant_with_retry("tt1", "5G", "10G", false/*oracle_mode*/, 8));
      OK(get_curr_simple_server().init_sql_proxy2());
      new_tenant_created = true;
      OK(get_tenant_id(run_ctx_.tenant_id_));
      ASSERT_NE(0, run_ctx_.tenant_id_);
      {
        share::ObTenantSwitchGuard tguard;
        OK(tguard.switch_to(run_ctx_.tenant_id_));
        OK(TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
        OK(sys_exe_sql("alter system set _shared_advance_checkpoint_interval = '1m' tenant tt1;"));
        OK(sys_exe_sql("alter system set_tp tp_name = EN_COMPACTION_SS_MINOR_MERGE_FAST_SKIP,error_code = 4016,frequency = 1;"));
        // clean up stale test data from previous runs
        exe_sql("delete from oceanbase.__all_shared_meta where meta_type = 2");
        // create a table via SQL to get a local tablet
        OK(exe_sql("create table if not exists test_svc2_table (a int)"));
        LOG_INFO("create table test_svc2_table done");
        OK(exe_sql("create table if not exists test_svc2_floor_table (a int)"));
        LOG_INFO("create table test_svc2_floor_table done");
        OK(exe_sql("create table if not exists test_svc2_all_del_table (a int)"));
        LOG_INFO("create table test_svc2_all_del_table done");
      }
      LOG_INFO("finish to create tt1 tenant");
    } else {
      OK(get_tenant_id(run_ctx_.tenant_id_));
      ASSERT_NE(0, run_ctx_.tenant_id_);
    }
    // Stop the LS garbage collector so directly-created LS are not reaped as
    // orphaned.  (Mirrors test_shared_meta_service.cpp.)
    {
      share::ObTenantSwitchGuard tguard;
      OK(tguard.switch_to(run_ctx_.tenant_id_));
      logservice::ObGarbageCollector *gc_service = MTL(logservice::ObGarbageCollector *);
      ASSERT_NE(nullptr, gc_service);
      gc_service->stop_create_new_gc_task_ = true;
    }
  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

  int get_tablet_and_ls_id(const char *table_name,
                           ObTabletID &tablet_id,
                           ObLSID &ls_id)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    uint64_t uid = 0;
    int64_t id = 0;
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    if (OB_FAIL(sql.assign_fmt(
            "select tablet_id from oceanbase.__all_virtual_table where table_name='%s';",
            table_name))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
        if (OB_FAIL(sql_proxy.read(res1, sql.ptr()))) {
          LOG_WARN("fail to read tablet_id", KR(ret));
        } else {
          sqlclient::ObMySQLResult *result = res1.get_result();
          if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(result->next())) {
            LOG_WARN("fail to get next row", KR(ret));
          } else if (OB_FAIL(result->get_uint("tablet_id", uid))) {
            LOG_WARN("fail to get tablet_id", KR(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      tablet_id = ObTabletID(uid);
      sql.reset();
      if (OB_FAIL(sql.assign_fmt(
              "select ls_id from oceanbase.__all_tablet_to_ls where tablet_id=%ld;",
              uid))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
          if (OB_FAIL(sql_proxy.read(res2, sql.ptr()))) {
            LOG_WARN("fail to read ls_id", KR(ret));
          } else {
            sqlclient::ObMySQLResult *result = res2.get_result();
            if (OB_ISNULL(result)) {
              ret = OB_ERR_UNEXPECTED;
            } else if (OB_FAIL(result->next())) {
              LOG_WARN("fail to get next row", KR(ret));
            } else if (OB_FAIL(result->get_int("ls_id", id))) {
              LOG_WARN("fail to get ls_id", KR(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        ls_id = ObLSID(id);
      }
    }
    return ret;
  }

  // Delete a single tablet_meta row (by full primary key) for cleanup.
  int delete_tablet_meta_row(ObSSMetaService &meta_svr,
                             const ObSSMetaTabletKey &key)
  {
    ObSSMetaDeleteParamV2 delete_param;
    delete_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_KEY,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        key.ls_id_, key.tablet_id_, key.meta_scn_, key.reorganization_scn_,
        true /*is_in_table*/);
    return meta_svr.delete_meta_row(delete_param);
  }

  // Read the tablet version at src_meta_scn, override its max_replayed_scn
  // (= MAX3(clog_checkpoint_scn_/ddl_checkpoint_scn_/mds_start_scn_)) to
  // target_replayed_scn, then re-serialize the whole ObSSTabletMetaValue and
  // insert it as a new version keyed at dst_meta_scn.
  int make_version_with_replayed_scn(ObSSMetaService &meta_svr,
      const ObLSID &ls_id, const ObTabletID &tablet_id, const share::SCN &reorgan_scn,
      const share::SCN &src_meta_scn, const share::SCN &dst_meta_scn,
      const share::SCN &target_replayed_scn)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator alloc("MkVer");
    ObTabletHandle hdl;
    ObTablet *tablet = nullptr;
    uint64_t data_version = 0;
    ObTabletMacroInfo *macro_info = nullptr;
    if (OB_FAIL(meta_svr.get_tablet(ls_id, tablet_id, reorgan_scn, src_meta_scn, alloc, hdl))) {
      LOG_WARN("fail to get tablet", KR(ret), K(ls_id), K(tablet_id), K(src_meta_scn));
    } else if (OB_ISNULL(tablet = hdl.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is null", KR(ret));
    } else if (FALSE_IT(tablet->tablet_meta_.clog_checkpoint_scn_ = target_replayed_scn)) {
    } else if (FALSE_IT(tablet->tablet_meta_.ddl_checkpoint_scn_ = target_replayed_scn)) {
    } else if (FALSE_IT(tablet->tablet_meta_.mds_start_scn_ = target_replayed_scn)) {
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
      LOG_WARN("fail to get data version", KR(ret));
    } else if (OB_FAIL(tablet->load_macro_info(0/*ls_epoch*/, alloc, macro_info))) {
      LOG_WARN("fail to load macro info", KR(ret));
    } else {
      ObSSTabletMetaValue value;
      if (OB_FAIL(value.set_tablet_info(data_version, macro_info, tablet))) {
        LOG_WARN("fail to set tablet info", KR(ret));
      } else {
        const int64_t sz = value.get_serialize_size();
        char *buf = static_cast<char *>(alloc.alloc(sz));
        int64_t pos = 0;
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc buf", KR(ret), K(sz));
        } else if (OB_FAIL(value.serialize(buf, sz, pos))) {
          LOG_WARN("fail to serialize value", KR(ret), K(sz));
        } else {
          ObSSMetaTabletKey key;
          key.ls_id_ = ls_id;
          key.tablet_id_ = tablet_id;
          key.reorganization_scn_ = reorgan_scn;
          key.meta_scn_ = dst_meta_scn;
          ObSSMetaTabletRow row(static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META),
                                key, ObString(static_cast<int32_t>(pos), buf));
          if (OB_FAIL(meta_svr.insert_tablet_meta(row))) {
            LOG_WARN("fail to insert tablet meta", KR(ret), K(key));
          }
        }
      }
    }
    return ret;
  }

  int read_tablet_lock_row(const ObLSID &ls_id,
                           const ObTabletID &tablet_id,
                           const share::SCN &reorgan_scn,
                           ObSSMetaCtrlLockRowValue &lock_row_value)
  {
    int ret = OB_SUCCESS;
    ObSSMetaCtrlKey tablet_meta_key;
    ObSSMetaCtrlKey tablet_lock_key;
    ObLockAtomicProtocol lock_row;
    common::ObISQLClient &sql_proxy =
        get_curr_simple_server().get_observer().get_mysql_proxy();
    tablet_meta_key.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
    tablet_meta_key.tablet_key_.tablet_id_ = tablet_id;
    tablet_meta_key.tablet_key_.reorg_scn_ = reorgan_scn;
    tablet_meta_key.tablet_key_.control_scn_ = share::SCN::min_scn();
    if (OB_FAIL(tablet_meta_key.get_lock_row_key(tablet_lock_key))) {
      LOG_WARN("fail to get tablet lock key", KR(ret), K(tablet_meta_key));
    } else if (OB_FAIL(lock_row.init(MTL_ID(), ls_id, tablet_lock_key))) {
      LOG_WARN("fail to init tablet lock protocol", KR(ret), K(ls_id), K(tablet_lock_key));
    } else if (OB_FAIL(lock_row.read_lock_row(sql_proxy, lock_row_value))) {
      LOG_WARN("fail to read tablet lock row", KR(ret), K(ls_id), K(tablet_lock_key));
    }
    return ret;
  }

  int persist_tablet_kept_meta_scn(const ObLSID &ls_id,
                                   const ObTabletID &tablet_id,
                                   const share::SCN &reorgan_scn,
                                   const share::SCN &kept_meta_scn)
  {
    int ret = OB_SUCCESS;
    ObSSMetaCtrlKey tablet_meta_key;
    ObSSMetaCtrlKey tablet_lock_key;
    ObLockAtomicProtocol lock_row;
    common::ObTimeoutCtx timeout_ctx;
    common::ObMySQLTransaction trans;
    bool trans_started = false;
    tablet_meta_key.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
    tablet_meta_key.tablet_key_.tablet_id_ = tablet_id;
    tablet_meta_key.tablet_key_.reorg_scn_ = reorgan_scn;
    tablet_meta_key.tablet_key_.control_scn_ = share::SCN::min_scn();
    if (OB_FAIL(tablet_meta_key.get_lock_row_key(tablet_lock_key))) {
      LOG_WARN("fail to get tablet lock key", KR(ret), K(tablet_meta_key));
    } else if (OB_FAIL(lock_row.init(MTL_ID(), ls_id, tablet_lock_key))) {
      LOG_WARN("fail to init tablet lock protocol", KR(ret), K(ls_id), K(tablet_lock_key));
    } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
                   MTL_ID(), ObAtomicProtocolUtil::DEFAULT_ATOMIC_PROTOCOL_TRANS_TIMEOUT_US,
                   false /*with_snapshot*/, OBCG_SHARED_META, timeout_ctx, trans))) {
      LOG_WARN("fail to start transaction", KR(ret), K(ls_id), K(tablet_lock_key));
    } else {
      trans_started = true;
      if (OB_FAIL(lock_row.persist_kept_meta_scn(kept_meta_scn, trans))) {
        LOG_WARN("fail to persist tablet kept meta scn", KR(ret), K(ls_id),
            K(tablet_lock_key), K(kept_meta_scn));
      }
    }
    if (trans_started) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObAtomicProtocolUtil::commit_trans(ret, trans))) {
        LOG_WARN("fail to finish transaction", KR(tmp_ret), KR(ret), K(ls_id), K(tablet_lock_key));
        ret = (OB_SUCCESS == ret ? tmp_ret : ret);
      }
    }
    return ret;
  }

  int mark_tablet_lock_row_deleted(const ObLSID &ls_id,
                                   const ObTabletID &tablet_id,
                                   const share::SCN &reorgan_scn,
                                   const share::SCN &mark_delete_meta_scn)
  {
    int ret = OB_SUCCESS;
    ObSSMetaCtrlKey tablet_meta_key;
    ObSSMetaCtrlKey tablet_lock_key;
    ObSSMetaCtrlLockRowValue lock_row_value;
    ObLockAtomicProtocol lock_row;
    common::ObISQLClient &sql_proxy =
        get_curr_simple_server().get_observer().get_mysql_proxy();
    tablet_meta_key.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
    tablet_meta_key.tablet_key_.tablet_id_ = tablet_id;
    tablet_meta_key.tablet_key_.reorg_scn_ = reorgan_scn;
    tablet_meta_key.tablet_key_.control_scn_ = share::SCN::min_scn();
    if (OB_FAIL(tablet_meta_key.get_lock_row_key(tablet_lock_key))) {
      LOG_WARN("fail to get tablet lock key", KR(ret), K(tablet_meta_key));
    } else if (OB_FAIL(lock_row.init(MTL_ID(), ls_id, tablet_lock_key))) {
      LOG_WARN("fail to init tablet lock protocol", KR(ret), K(ls_id), K(tablet_lock_key));
    } else if (OB_FAIL(lock_row.read_lock_row(sql_proxy, lock_row_value))) {
      LOG_WARN("fail to read tablet lock row", KR(ret), K(ls_id), K(tablet_lock_key));
    } else if (!lock_row_value.kept_meta_scn_.is_min()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("tablet lock row kept meta scn is already persisted", KR(ret),
          K(ls_id), K(tablet_lock_key), K(lock_row_value));
    } else {
      // 模拟 legacy mark-delete 行：业务字段进入终态，但尚未持久化 kept_meta_scn。
      lock_row_value.is_mark_deleted_ = true;
      lock_row_value.meta_scn_ = mark_delete_meta_scn;
      lock_row_value.deleted_scn_ = mark_delete_meta_scn;
      lock_row_value.last_major_snapshot_ = 0;
      if (OB_FAIL(lock_row.update_lock_row(lock_row_value, sql_proxy))) {
        LOG_WARN("fail to mark tablet lock row deleted", KR(ret), K(ls_id),
            K(tablet_lock_key), K(lock_row_value));
      }
    }
    return ret;
  }

  int init_kept_meta_scn_ctx(const ObSSGCTablet &gc_tablet,
                             ObSSMarkSweepCtx &ctx)
  {
    int ret = OB_SUCCESS;
    ctx.tenant_id_ = MTL_ID();
    if (OB_UNLIKELY(!gc_tablet.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid gc tablet", KR(ret), K(gc_tablet));
    } else if (OB_FAIL(ctx.tablet_with_meta_scn_arr_.push_back(
                   ObSSGCTabletWithMetaScn(gc_tablet, share::SCN::min_scn())))) {
      LOG_WARN("fail to append gc tablet", KR(ret), K(gc_tablet));
    } else if (OB_FAIL(ctx.tablet_kept_meta_scn_map_.create(
                   2, ObMemAttr(ctx.tenant_id_, "TestKeptMap")))) {
      LOG_WARN("fail to create tablet kept meta scn map", KR(ret), K(gc_tablet));
    } else if (OB_FAIL(ctx.mark_deleted_tablets_.create(
                   2, ObMemAttr(ctx.tenant_id_, "TestMarkMap")))) {
      LOG_WARN("fail to create mark deleted tablet map", KR(ret), K(gc_tablet));
    }
    return ret;
  }

  int refresh_all_kept_meta_scns(ObSSMarkSweepCtx &ctx)
  {
    ctx.tablet_kept_meta_scn_map_.reuse();
    ObSSMarkSweepPrepareTask prepare_task;
    return prepare_task.get_all_kept_meta_scns_(ctx);
  }

protected:
  static TestRunCtx run_ctx_;
};

TestRunCtx TestSSGCDelAndKeptCalculation::run_ctx_;

TEST_F(TestSSGCDelAndKeptCalculation, final_min_reuses_committed_kept_meta_scn)
{
  share::ObTenantSwitchGuard tg;
  OK(tg.switch_to(run_ctx_.tenant_id_));
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  ObTabletID tablet_id;
  ObLSID ls_id;
  OK(get_tablet_and_ls_id("test_svc2_floor_table", tablet_id, ls_id));
  const share::SCN reorgan = share::SCN::min_scn();
  OK(meta_svr->create_tablet(ls_id, tablet_id, reorgan, true /*check_lease*/));

  share::SCN base_meta_scn;
  OK(meta_svr->get_tablet_latest_version(ls_id, tablet_id, reorgan, base_meta_scn));
  const share::SCN V1 = share::SCN::plus(base_meta_scn, 100);
  const share::SCN V2 = share::SCN::plus(base_meta_scn, 200);
  const share::SCN V3 = share::SCN::plus(base_meta_scn, 300);
  const share::SCN V4 = share::SCN::plus(base_meta_scn, 400);
  share::SCN small_replayed_scn;
  share::SCN checkpoint_scn;
  OK(small_replayed_scn.convert_for_logservice(1000));
  OK(checkpoint_scn.convert_for_logservice(2000));
  OK(make_version_with_replayed_scn(*meta_svr, ls_id, tablet_id, reorgan,
      base_meta_scn, V1, small_replayed_scn));
  OK(make_version_with_replayed_scn(*meta_svr, ls_id, tablet_id, reorgan,
      base_meta_scn, V2, small_replayed_scn));
  OK(make_version_with_replayed_scn(*meta_svr, ls_id, tablet_id, reorgan,
      base_meta_scn, V3, small_replayed_scn));
  OK(persist_tablet_kept_meta_scn(ls_id, tablet_id, reorgan, V2));

  ObSEArray<share::SCN, 4> reserved_meta_scns;
  ObSEArray<share::SCN, 4> all_meta_scns;
  OK(all_meta_scns.push_back(V1));
  OK(all_meta_scns.push_back(V2));
  OK(all_meta_scns.push_back(V3));
  common::hash::ObHashMap<ObLSID, share::SCN> checkpoint_map;
  OK(checkpoint_map.create(4, "FloorCkpt"));
  OK(checkpoint_map.set_refactored(ls_id, checkpoint_scn));
  const share::SCN tablet_meta_scn = share::SCN::min_scn();
  const share::SCN invalid_mark_delete_meta_scn;
  const share::SCN safe_meta_scn = share::SCN::plus(base_meta_scn, 500);
  const ObSSGCTablet gc_tablet(ls_id, tablet_id, reorgan);
  ObSSMarkSweepSortTask task;
  ObSSMarkSweepCtx ctx;
  share::SCN loaded_kept_meta_scn;
  OK(init_kept_meta_scn_ctx(gc_tablet, ctx));
  OK(refresh_all_kept_meta_scns(ctx));
  OK(ctx.get_tablet_kept_meta_scn(gc_tablet, loaded_kept_meta_scn));
  ASSERT_EQ(V2, loaded_kept_meta_scn);
  ObSSGetDelKeptMetaScnParam param(gc_tablet,
      tablet_meta_scn, loaded_kept_meta_scn, safe_meta_scn,
      invalid_mark_delete_meta_scn, reserved_meta_scns, all_meta_scns, checkpoint_map);

  ObSEArray<share::SCN, 4> del_meta_scns;
  ObSEArray<share::SCN, 4> kept_meta_scns;
  OK(task.get_del_and_kept_meta_scns_simply_(param, del_meta_scns, kept_meta_scns));
  // 最终计算结果为 min 时，应使用已提交的 V2 作为分类边界。
  ASSERT_EQ(1, del_meta_scns.count());
  ASSERT_EQ(V1, del_meta_scns.at(0));
  ASSERT_EQ(2, kept_meta_scns.count());
  ASSERT_EQ(V2, kept_meta_scns.at(0));
  ASSERT_EQ(V3, kept_meta_scns.at(1));

  // 已提交位点等于快照最大版本时，本轮仍可安全分类并保留该版本。
  OK(persist_tablet_kept_meta_scn(ls_id, tablet_id, reorgan, V3));
  OK(refresh_all_kept_meta_scns(ctx));
  OK(ctx.get_tablet_kept_meta_scn(gc_tablet, loaded_kept_meta_scn));
  ASSERT_EQ(V3, loaded_kept_meta_scn);
  param.tablet_kept_meta_scn_ = loaded_kept_meta_scn;
  del_meta_scns.reuse();
  kept_meta_scns.reuse();
  OK(task.get_del_and_kept_meta_scns_simply_(param, del_meta_scns, kept_meta_scns));
  ASSERT_EQ(2, del_meta_scns.count());
  ASSERT_EQ(V1, del_meta_scns.at(0));
  ASSERT_EQ(V2, del_meta_scns.at(1));
  ASSERT_EQ(1, kept_meta_scns.count());
  ASSERT_EQ(V3, kept_meta_scns.at(0));

  // 模拟新轮次在本轮冻结快照之后提交 V4，旧快照必须终止而不得产生删除计划。
  OK(make_version_with_replayed_scn(*meta_svr, ls_id, tablet_id, reorgan,
      base_meta_scn, V4, small_replayed_scn));
  OK(persist_tablet_kept_meta_scn(ls_id, tablet_id, reorgan, V4));
  ObSSMetaCtrlLockRowValue lock_row_value;
  OK(read_tablet_lock_row(ls_id, tablet_id, reorgan, lock_row_value));
  ASSERT_EQ(V4, lock_row_value.kept_meta_scn_);
  ASSERT_EQ(V3, all_meta_scns.at(all_meta_scns.count() - 1));
  OK(refresh_all_kept_meta_scns(ctx));
  OK(ctx.get_tablet_kept_meta_scn(gc_tablet, loaded_kept_meta_scn));
  ASSERT_EQ(V4, loaded_kept_meta_scn);
  param.tablet_kept_meta_scn_ = loaded_kept_meta_scn;
  del_meta_scns.reuse();
  kept_meta_scns.reuse();
  ASSERT_EQ(OB_CANCELED,
      task.get_del_and_kept_meta_scns_simply_(param, del_meta_scns, kept_meta_scns));
  ASSERT_TRUE(del_meta_scns.empty());
  ASSERT_TRUE(kept_meta_scns.empty());

  // 新轮次已将位点推进到 V4，旧轮次的 V3 持久化计划应直接取消。
  ObSSMarkSweepSortTask::ObSSPersistKeptMetaScnItem stale_item;
  ObSEArray<ObSSMarkSweepSortTask::ObSSPersistKeptMetaScnItem, 1> stale_items;
  ObSEArray<ObSSMarkSweepSortTask::ObSSPersistKeptMetaScnItem, 1> filtered_items;
  OK(task.build_kept_meta_scn_persist_item_(gc_tablet, V3, stale_item));
  OK(stale_items.push_back(stale_item));
  ASSERT_EQ(OB_CANCELED,
      task.filter_kept_meta_scn_items_(stale_items, ctx, filtered_items));
  ASSERT_TRUE(filtered_items.empty());
  ASSERT_EQ(OB_CANCELED,
      persist_tablet_kept_meta_scn(ls_id, tablet_id, reorgan, V3));
  OK(read_tablet_lock_row(ls_id, tablet_id, reorgan, lock_row_value));
  ASSERT_EQ(V4, lock_row_value.kept_meta_scn_);
  OK(checkpoint_map.destroy());
}

TEST_F(TestSSGCDelAndKeptCalculation, missing_lock_row_cancels_before_persist)
{
  share::ObTenantSwitchGuard tg;
  OK(tg.switch_to(run_ctx_.tenant_id_));
  ObTabletID existing_tablet_id;
  ObLSID ls_id;
  OK(get_tablet_and_ls_id("test_svc2_floor_table", existing_tablet_id, ls_id));

  const ObTabletID missing_tablet_id(
      ObTabletID::MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID - 1011);
  const share::SCN reorgan = share::SCN::min_scn();
  share::SCN kept_meta_scn;
  OK(kept_meta_scn.convert_for_logservice(4321));
  const ObSSGCTablet missing_tablet(ls_id, missing_tablet_id, reorgan);
  ObSSMarkSweepSortTask task;
  ObSSMarkSweepCtx ctx;
  share::SCN loaded_kept_meta_scn;
  OK(init_kept_meta_scn_ctx(missing_tablet, ctx));
  OK(refresh_all_kept_meta_scns(ctx));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      ctx.get_tablet_kept_meta_scn(missing_tablet, loaded_kept_meta_scn));
  ASSERT_FALSE(loaded_kept_meta_scn.is_valid());

  ObSSMarkSweepSortTask::ObSSPersistKeptMetaScnItem item;
  ObSEArray<ObSSMarkSweepSortTask::ObSSPersistKeptMetaScnItem, 1> items;
  ObSEArray<ObSSMarkSweepSortTask::ObSSPersistKeptMetaScnItem, 1> filtered_items;
  OK(task.build_kept_meta_scn_persist_item_(missing_tablet, kept_meta_scn, item));
  OK(items.push_back(item));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      task.filter_kept_meta_scn_items_(items, ctx, filtered_items));
  ASSERT_TRUE(filtered_items.empty());

  // map 中记录的 min 位点不是缺行，仍应进入事务候选。
  OK(ctx.tablet_kept_meta_scn_map_.set_refactored(missing_tablet, share::SCN::min_scn()));
  filtered_items.reuse();
  OK(task.filter_kept_meta_scn_items_(items, ctx, filtered_items));
  ASSERT_EQ(1, filtered_items.count());
}

// mark-delete 的 tablet 所有版本（含 mark_delete 版本自身）都被判定可删时，kept_meta_scns
// 为空，此时必须把 Lock Row 位点推进到 mark_delete_meta_scn + 1。否则位点停留在旧值，
// 下一轮会以旧位点为边界把这些已删版本重新判定为 kept，kept 宏块集合相对上一轮回退。
TEST_F(TestSSGCDelAndKeptCalculation, all_versions_deleted_advances_kept_meta_scn)
{
  LOG_INFO("[TEST CASE BEGIN] all_versions_deleted_advances_kept_meta_scn");
  share::ObTenantSwitchGuard tg;
  OK(tg.switch_to(run_ctx_.tenant_id_));
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  ObTabletID tablet_id;
  ObLSID ls_id;
  OK(get_tablet_and_ls_id("test_svc2_all_del_table", tablet_id, ls_id));
  const share::SCN reorgan = share::SCN::min_scn();
  OK(meta_svr->create_tablet(ls_id, tablet_id, reorgan, true /*check_lease*/));

  share::SCN v_base;
  OK(meta_svr->get_tablet_latest_version(ls_id, tablet_id, reorgan, v_base));
  const share::SCN V1 = share::SCN::plus(v_base, 100);
  const share::SCN V2 = share::SCN::plus(v_base, 200);
  const share::SCN V3 = share::SCN::plus(v_base, 300);
  const share::SCN big = share::SCN::plus(v_base, 400);
  const share::SCN terminal_kept_meta_scn = share::SCN::plus(V3, 1);
  share::SCN small_rep;
  share::SCN big_ckpt;
  OK(small_rep.convert_for_logservice(1000));
  OK(big_ckpt.convert_for_logservice(9000));
  OK(make_version_with_replayed_scn(*meta_svr, ls_id, tablet_id, reorgan, v_base, V1, small_rep));
  OK(make_version_with_replayed_scn(*meta_svr, ls_id, tablet_id, reorgan, v_base, V2, small_rep));
  OK(make_version_with_replayed_scn(*meta_svr, ls_id, tablet_id, reorgan, v_base, V3, small_rep));
  // 生产路径会扫描快照内全部版本，仅保留三个回放位点可控的合成版本。
  ObSSMetaTabletKey base_key;
  base_key.ls_id_ = ls_id;
  base_key.tablet_id_ = tablet_id;
  base_key.reorganization_scn_ = reorgan;
  base_key.meta_scn_ = v_base;
  OK(delete_tablet_meta_row(*meta_svr, base_key));
  // V3 为 mark-delete 版本，Lock Row 保留 legacy 的 min 位点。
  OK(mark_tablet_lock_row_deleted(ls_id, tablet_id, reorgan, V3));

  ObSEArray<share::SCN, 4> reserved_meta_scns; // empty
  ObSEArray<share::SCN, 4> all_meta_scns;      // ascending
  OK(all_meta_scns.push_back(V1));
  OK(all_meta_scns.push_back(V2));
  OK(all_meta_scns.push_back(V3));
  const ObSSGCTablet gc_tablet(ls_id, tablet_id, reorgan);
  ObSSMarkSweepSortTask task;
  ObSSMarkSweepCtx ctx;
  share::SCN loaded_kept_meta_scn;
  OK(init_kept_meta_scn_ctx(gc_tablet, ctx));
  ctx.snapshot_meta_scn_ = big;
  ctx.safe_meta_scn_ = big;
  OK(ctx.reserved_meta_scns_.push_back(big));
  OK(ctx.ls_ss_checkpoint_scn_map_.create(4, "AllDelCkpt"));
  OK(ctx.ls_ss_checkpoint_scn_map_.set_refactored(ls_id, big_ckpt));
  OK(ctx.pipeline_.init(ctx.tenant_id_, ObSSMarkSweepDagParam::DEFAULT_MEMORY_LIMIT,
      ObSSMarkSweepDagParam::DEFAULT_FILE_BUF_SIZE, 0 /*expire_timestamp*/, &ctx.compare_));
  OK(ctx.mark_deleted_tablets_.set_refactored(gc_tablet, V3));
  OK(refresh_all_kept_meta_scns(ctx));
  OK(ctx.get_tablet_kept_meta_scn(gc_tablet, loaded_kept_meta_scn));
  ASSERT_TRUE(loaded_kept_meta_scn.is_min());

  // studied/safe 均为 big，reserved=big 回落到真实版本 V3，
  // 仅由 real_min_reserved == mark_delete_meta_scn 触发 can_del_all。
  const ObSSGCTabletWithMetaScn tablet_with_meta_scn(gc_tablet, big);
  ObSSMarkSweepSortTask::ObSSPersistKeptMetaScnItem persist_item;
  {
    ObSSMarkSweepSortTask::ObMarkSweepSorterPair sorter_pair;
    OK(sorter_pair.init(ctx.pipeline_));
    OK(task.process_one_tablet_(
        ctx, tablet_with_meta_scn, loaded_kept_meta_scn, sorter_pair, persist_item));
    OK(sorter_pair.finish_and_add_file());
  }
  ASSERT_TRUE(persist_item.is_valid());
  ASSERT_EQ(terminal_kept_meta_scn, persist_item.kept_meta_scn_);
  ObSEArray<ObSSMarkSweepSortTask::ObSSPersistKeptMetaScnItem, 1> persist_items;
  ObSEArray<ObSSMarkSweepSortTask::ObSSPersistKeptMetaScnItem, 1> filtered_items;
  OK(persist_items.push_back(persist_item));
  OK(task.filter_kept_meta_scn_items_(persist_items, ctx, filtered_items));
  ASSERT_EQ(1, filtered_items.count());
  OK(task.persist_kept_meta_scn_with_trans_(filtered_items.at(0), ctx));
  ObSSMetaCtrlLockRowValue lock_row_value;
  OK(read_tablet_lock_row(ls_id, tablet_id, reorgan, lock_row_value));
  ASSERT_TRUE(lock_row_value.is_mark_deleted_);
  ASSERT_EQ(terminal_kept_meta_scn, lock_row_value.kept_meta_scn_);

  // 第二轮：终态位点与 reserved 同时存在时，diff 未删净的版本仍应全部判定为可删，
  // 既不能被 reserved 复活，也不能因 min_meta_scn > mark_delete_meta_scn 报错。
  OK(refresh_all_kept_meta_scns(ctx));
  OK(ctx.get_tablet_kept_meta_scn(gc_tablet, loaded_kept_meta_scn));
  ASSERT_EQ(terminal_kept_meta_scn, loaded_kept_meta_scn);
  const share::SCN min_tablet_meta_scn = share::SCN::min_scn();
  ObSSGetDelKeptMetaScnParam next_param(gc_tablet,
      min_tablet_meta_scn /*走 Lock Row 位点*/,
      loaded_kept_meta_scn, big /*safe_meta_scn*/,
      V3 /*mark_delete_meta_scn*/, ctx.reserved_meta_scns_, all_meta_scns,
      ctx.ls_ss_checkpoint_scn_map_);
  ObSEArray<share::SCN, 4> next_del_meta_scns;
  ObSEArray<share::SCN, 4> next_kept_meta_scns;
  OK(task.get_del_and_kept_meta_scns_simply_(
      next_param, next_del_meta_scns, next_kept_meta_scns));
  ASSERT_EQ(3, next_del_meta_scns.count());
  ASSERT_EQ(V3, next_del_meta_scns.at(2));
  ASSERT_TRUE(next_kept_meta_scns.empty());

  // 只有 mark_delete_meta_scn + 1 是合法终态，继续越过必须报错。
  const share::SCN invalid_terminal_kept_meta_scn =
      share::SCN::plus(terminal_kept_meta_scn, 1);
  ObSSGetDelKeptMetaScnParam invalid_floor_param(gc_tablet,
      min_tablet_meta_scn, invalid_terminal_kept_meta_scn,
      big /*safe_meta_scn*/, V3 /*mark_delete_meta_scn*/, reserved_meta_scns,
      all_meta_scns, ctx.ls_ss_checkpoint_scn_map_);
  ObSEArray<share::SCN, 4> invalid_floor_del_meta_scns;
  ObSEArray<share::SCN, 4> invalid_floor_kept_meta_scns;
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.get_del_and_kept_meta_scns_simply_(
      invalid_floor_param, invalid_floor_del_meta_scns, invalid_floor_kept_meta_scns));
  ASSERT_TRUE(invalid_floor_del_meta_scns.empty());
  ASSERT_TRUE(invalid_floor_kept_meta_scns.empty());

  // 终态位点是幂等的：再次计算得到同一位点，过滤后不会重复进入事务。
  share::SCN next_kept_meta_scn;
  OK(task.calc_kept_meta_scn_(
      next_param, next_del_meta_scns, next_kept_meta_scns, next_kept_meta_scn));
  ASSERT_EQ(terminal_kept_meta_scn, next_kept_meta_scn);
  filtered_items.reuse();
  OK(task.filter_kept_meta_scn_items_(persist_items, ctx, filtered_items));
  ASSERT_TRUE(filtered_items.empty());

  // 未 mark-delete 的 tablet 不应出现所有版本都落到 del 的情况。
  const share::SCN invalid_mark_delete_meta_scn;
  ObSSGetDelKeptMetaScnParam no_mark_delete_param(gc_tablet,
      big /*tablet_meta_scn*/, loaded_kept_meta_scn, big /*safe_meta_scn*/,
      invalid_mark_delete_meta_scn, reserved_meta_scns, all_meta_scns,
      ctx.ls_ss_checkpoint_scn_map_);
  ObSEArray<share::SCN, 4> no_mark_del;
  ObSEArray<share::SCN, 4> no_mark_kept;
  OK(task.get_del_and_kept_meta_scns_simply_(no_mark_delete_param, no_mark_del, no_mark_kept));
  ASSERT_EQ(3, no_mark_del.count());
  ASSERT_TRUE(no_mark_kept.empty());
  share::SCN no_mark_kept_meta_scn;
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.calc_kept_meta_scn_(
      no_mark_delete_param, no_mark_del, no_mark_kept, no_mark_kept_meta_scn));
  ASSERT_FALSE(no_mark_kept_meta_scn.is_valid());

  // 快照为空（版本已被并发 GC 删净）时同样不推进位点。
  ObSEArray<share::SCN, 4> empty_meta_scns;
  share::SCN empty_kept_meta_scn;
  OK(task.calc_kept_meta_scn_(
      next_param, empty_meta_scns, empty_meta_scns, empty_kept_meta_scn));
  ASSERT_FALSE(empty_kept_meta_scn.is_valid());

  const share::SCN versions[] = {V1, V2, V3};
  for (int64_t i = 0; i < 3; ++i) {
    ObSSMetaTabletKey k;
    k.ls_id_ = ls_id;
    k.tablet_id_ = tablet_id;
    k.reorganization_scn_ = reorgan;
    k.meta_scn_ = versions[i];
    OK(delete_tablet_meta_row(*meta_svr, k));
  }
  LOG_INFO("[TEST CASE END] all_versions_deleted_advances_kept_meta_scn");
}

// Covers ObSSMarkSweepSortTask::get_del_and_kept_meta_scns_simply_ Rule 3:
// a version below min_meta_scn (and not the force-kept last one) is deleted
// iff its tablet max_replayed_scn < ls_ss_checkpoint_scn.
TEST_F(TestSSGCDelAndKeptCalculation, rule3_max_replayed_scn_vs_ls_ss_checkpoint_scn)
{
  LOG_INFO("[TEST CASE BEGIN] rule3_max_replayed_scn_vs_ls_ss_checkpoint_scn");
  share::ObTenantSwitchGuard tg;
  OK(tg.switch_to(run_ctx_.tenant_id_));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  ObTabletID tablet_id;
  ObLSID ls_id;
  OK(get_tablet_and_ls_id("test_svc2_table", tablet_id, ls_id));
  ASSERT_TRUE(tablet_id.is_valid());
  ASSERT_TRUE(ls_id.is_valid());

  const share::SCN reorgan = share::SCN::min_scn();
  OK(meta_svr->create_tablet(ls_id, tablet_id, reorgan, true/*check_lease*/));

  share::SCN v_base;
  OK(meta_svr->get_tablet_latest_version(ls_id, tablet_id, reorgan, v_base));
  ASSERT_TRUE(v_base.is_valid());

  // Three synthetic versions at increasing meta_scns, all derived from the
  // create_tablet version but with different (overridden) max_replayed_scns.
  const share::SCN V1 = share::SCN::plus(v_base, 100);
  const share::SCN V2 = share::SCN::plus(v_base, 200);
  const share::SCN V3 = share::SCN::plus(v_base, 300);

  share::SCN small_rep;
  share::SCN large_rep;
  share::SCN ckpt;
  OK(small_rep.convert_for_logservice(1000));
  OK(large_rep.convert_for_logservice(3000));
  OK(ckpt.convert_for_logservice(2000));

  OK(make_version_with_replayed_scn(*meta_svr, ls_id, tablet_id, reorgan, v_base, V1, small_rep));
  OK(make_version_with_replayed_scn(*meta_svr, ls_id, tablet_id, reorgan, v_base, V2, large_rep));
  OK(make_version_with_replayed_scn(*meta_svr, ls_id, tablet_id, reorgan, v_base, V3, large_rep));

  // Param: tablet_meta_scn = safe_meta_scn = big (> V3) so V1/V2/V3 are all
  // below min_meta_scn; reserved empty so it does not affect min_meta_scn.
  // is_mark_deleted = false so V3 (the largest meta_scn) is force-kept.
  ObSSGCTablet gc_tablet(ls_id, tablet_id, reorgan);
  common::hash::ObHashMap<ObLSID, share::SCN> ckpt_map;
  OK(ckpt_map.create(4, "Ckpt"));
  OK(ckpt_map.set_refactored(ls_id, ckpt));

  ObSEArray<share::SCN, 4> reserved_meta_scns; // empty
  ObSEArray<share::SCN, 4> all_meta_scns;       // must be ascending
  OK(all_meta_scns.push_back(V1));
  OK(all_meta_scns.push_back(V2));
  OK(all_meta_scns.push_back(V3));

  const share::SCN big = share::SCN::plus(v_base, 400);
  // param holds const refs; must be a named object, a temporary would dangle
  const share::SCN invalid_mark_delete_meta_scn;
  ObSSGetDelKeptMetaScnParam param(gc_tablet,
      big /*tablet_meta_scn*/, share::SCN::min_scn() /*unused prefetched kept_meta_scn*/,
      big /*safe_meta_scn*/,
      invalid_mark_delete_meta_scn /*not mark-deleted*/,
      reserved_meta_scns, all_meta_scns, ckpt_map);

  ObSSMarkSweepSortTask task;
  ObSEArray<share::SCN, 4> del_meta_scns;
  ObSEArray<share::SCN, 4> kept_meta_scns;
  OK(task.get_del_and_kept_meta_scns_simply_(
      param, del_meta_scns, kept_meta_scns));
  LOG_INFO("get_del_and_kept_meta_scns_simply_ done", K(del_meta_scns), K(kept_meta_scns),
      K(V1), K(V2), K(V3), K(ckpt), K(small_rep), K(large_rep));

  // V1: max_replayed_scn(1000) < ckpt(2000)  -> del (Rule 3, is_exceed=false)
  // V2: max_replayed_scn(3000) >= ckpt(2000) -> kept (Rule 3, is_exceed=true)
  // V3: largest meta_scn                      -> force-kept (Rule 2)
  ASSERT_EQ(1, del_meta_scns.count());
  ASSERT_EQ(V1, del_meta_scns.at(0));
  ASSERT_EQ(2, kept_meta_scns.count());
  ASSERT_EQ(V2, kept_meta_scns.at(0));
  ASSERT_EQ(V3, kept_meta_scns.at(1));

  // ---- Rule 2 extension: mark-deleted tablet ----
  // 将同一把 Lock Row 真实置为 mark-delete，且保留 legacy 的 min 位点。
  OK(mark_tablet_lock_row_deleted(ls_id, tablet_id, reorgan, V3));
  ObSSMetaCtrlLockRowValue marked_lock_row_value;
  OK(read_tablet_lock_row(ls_id, tablet_id, reorgan, marked_lock_row_value));
  ASSERT_FALSE(param.mark_delete_meta_scn_.is_valid());
  ASSERT_TRUE(marked_lock_row_value.is_mark_deleted_);
  ASSERT_TRUE(marked_lock_row_value.kept_meta_scn_.is_min());
  ObSSMarkSweepCtx marked_ctx;
  share::SCN marked_kept_meta_scn;
  OK(init_kept_meta_scn_ctx(gc_tablet, marked_ctx));
  OK(marked_ctx.mark_deleted_tablets_.set_refactored(gc_tablet, V3));
  OK(refresh_all_kept_meta_scns(marked_ctx));
  OK(marked_ctx.get_tablet_kept_meta_scn(gc_tablet, marked_kept_meta_scn));
  ASSERT_TRUE(marked_kept_meta_scn.is_min());

  {
    // 复用 DROP 前构造的 param，模拟 study 已冻结、Sort 前 Lock Row 才 mark-delete。
    ObSEArray<share::SCN, 4> del_after_drop;
    ObSEArray<share::SCN, 4> kept_after_drop;
    OK(task.get_del_and_kept_meta_scns_simply_(
        param, del_after_drop, kept_after_drop));
    ASSERT_EQ(1, del_after_drop.count());
    ASSERT_EQ(V1, del_after_drop.at(0));
    ASSERT_EQ(2, kept_after_drop.count());
    ASSERT_EQ(V2, kept_after_drop.at(0));
    ASSERT_EQ(V3, kept_after_drop.at(1));
    ASSERT_LT(marked_lock_row_value.kept_meta_scn_, kept_after_drop.at(0));
    ObSSMarkSweepSortTask::ObSSPersistKeptMetaScnItem persist_item;
    ObSEArray<ObSSMarkSweepSortTask::ObSSPersistKeptMetaScnItem, 1> persist_items;
    ObSEArray<ObSSMarkSweepSortTask::ObSSPersistKeptMetaScnItem, 1> filtered_items;
    OK(task.build_kept_meta_scn_persist_item_(gc_tablet, kept_after_drop.at(0), persist_item));
    OK(persist_items.push_back(persist_item));
    OK(task.filter_kept_meta_scn_items_(persist_items, marked_ctx, filtered_items));
    ASSERT_EQ(1, filtered_items.count());
    OK(task.persist_kept_meta_scn_with_trans_(filtered_items.at(0), marked_ctx));
    ObSSMetaCtrlLockRowValue lock_row_value_after_drop;
    OK(read_tablet_lock_row(ls_id, tablet_id, reorgan, lock_row_value_after_drop));
    ASSERT_TRUE(lock_row_value_after_drop.is_mark_deleted_);
    ASSERT_EQ(V2, lock_row_value_after_drop.kept_meta_scn_);
    ASSERT_EQ(marked_lock_row_value.op_id_, lock_row_value_after_drop.op_id_);
    ASSERT_EQ(marked_lock_row_value.meta_scn_, lock_row_value_after_drop.meta_scn_);
    ASSERT_EQ(marked_lock_row_value.deleted_scn_, lock_row_value_after_drop.deleted_scn_);
    ASSERT_EQ(marked_lock_row_value.last_major_snapshot_,
        lock_row_value_after_drop.last_major_snapshot_);
    ASSERT_EQ(marked_lock_row_value.transfer_seq_, lock_row_value_after_drop.transfer_seq_);
    ASSERT_EQ(OB_CANCELED,
        persist_tablet_kept_meta_scn(ls_id, tablet_id, reorgan, V1));
    OK(read_tablet_lock_row(ls_id, tablet_id, reorgan, lock_row_value_after_drop));
    ASSERT_TRUE(lock_row_value_after_drop.is_mark_deleted_);
    ASSERT_EQ(V2, lock_row_value_after_drop.kept_meta_scn_);
  }

  // min_meta_scn = MIN(tablet_meta_scn, safe_meta_scn, ...) = V3 == mark_delete_meta_scn
  // -> can_del_all: the mark-delete version itself (V3, == min and the last one) enters the
  // del candidates and passes the checkpoint check per its own max_replayed_scn.
  {
    ObSEArray<share::SCN, 4> del2;
    ObSEArray<share::SCN, 4> kept2;
    ObSSGetDelKeptMetaScnParam param2(gc_tablet,
        V3 /*tablet_meta_scn*/, share::SCN::min_scn() /*unused prefetched kept_meta_scn*/,
        big /*safe_meta_scn*/,
        V3 /*mark_delete_meta_scn == min_meta_scn*/,
        reserved_meta_scns, all_meta_scns, ckpt_map);
    OK(task.get_del_and_kept_meta_scns_simply_(
        param2, del2, kept2));
    LOG_INFO("mark_deleted can_del_all done", K(del2), K(kept2), K(V1), K(V2), K(V3));
    // V1: replayed(1000) < ckpt(2000)  -> del
    // V2: replayed(3000) >= ckpt(2000) -> kept (Rule 3)
    // V3: == min_meta_scn and can_del_all, replayed(3000) >= ckpt(2000) -> kept by Rule 3
    ASSERT_EQ(1, del2.count());
    ASSERT_EQ(V1, del2.at(0));
    ASSERT_EQ(2, kept2.count());
    ASSERT_EQ(V2, kept2.at(0));
    ASSERT_EQ(V3, kept2.at(1));
    ObSSMetaCtrlLockRowValue lock_row_value_after_gc;
    OK(read_tablet_lock_row(ls_id, tablet_id, reorgan, lock_row_value_after_gc));
    ASSERT_TRUE(lock_row_value_after_gc.is_mark_deleted_);
    ASSERT_EQ(V2, lock_row_value_after_gc.kept_meta_scn_);
  }
  {
    // mark-deleted but min_meta_scn < mark_delete_meta_scn (a reserved consumer still
    // pins an older version): the last version must stay force-kept.
    ObSEArray<share::SCN, 4> reserved4;
    OK(reserved4.push_back(V2)); // real_min_reserved = V2 -> min_meta_scn = V2 < V3
    share::SCN big_ckpt;
    OK(big_ckpt.convert_for_logservice(9000));
    common::hash::ObHashMap<ObLSID, share::SCN> big_ckpt_map;
    OK(big_ckpt_map.create(4, "Ckpt3"));
    OK(big_ckpt_map.set_refactored(ls_id, big_ckpt));
    ObSEArray<share::SCN, 4> del4;
    ObSEArray<share::SCN, 4> kept4;
    ObSSGetDelKeptMetaScnParam param4(gc_tablet,
        V3 /*tablet_meta_scn*/, share::SCN::min_scn() /*unused prefetched kept_meta_scn*/,
        big /*safe_meta_scn*/,
        V3 /*mark_delete_meta_scn > min_meta_scn(V2)*/,
        reserved4, all_meta_scns, big_ckpt_map);
    OK(task.get_del_and_kept_meta_scns_simply_(
        param4, del4, kept4));
    LOG_INFO("mark_deleted reserved_pins done", K(del4), K(kept4), K(V1), K(V2), K(V3));
    // V1 < min(V2), ckpt exceeds -> del; V2 == min but !can_del_all -> kept;
    // V3 force-kept (last version, !can_del_all).
    ASSERT_EQ(1, del4.count());
    ASSERT_EQ(V1, del4.at(0));
    ASSERT_EQ(2, kept4.count());
    ASSERT_EQ(V2, kept4.at(0));
    ASSERT_EQ(V3, kept4.at(1));
    OK(big_ckpt_map.destroy());
  }
  {
    // Same but ckpt advanced beyond all replayed scns -> everything is deletable,
    // including the mark-delete version V3 (no force-kept for a fully-consumed
    // mark-deleted tablet). 终态 max 必须放在该 tablet 的最后一个用例。
    share::SCN big_ckpt;
    OK(big_ckpt.convert_for_logservice(9000));
    common::hash::ObHashMap<ObLSID, share::SCN> big_ckpt_map;
    OK(big_ckpt_map.create(4, "Ckpt2"));
    OK(big_ckpt_map.set_refactored(ls_id, big_ckpt));
    ObSEArray<share::SCN, 4> del3;
    ObSEArray<share::SCN, 4> kept3;
    ObSSGetDelKeptMetaScnParam param3(gc_tablet,
        V3 /*tablet_meta_scn*/, share::SCN::min_scn() /*unused prefetched kept_meta_scn*/,
        big /*safe_meta_scn*/,
        V3 /*mark_delete_meta_scn == min_meta_scn*/,
        reserved_meta_scns, all_meta_scns, big_ckpt_map);
    OK(task.get_del_and_kept_meta_scns_simply_(
        param3, del3, kept3));
    LOG_INFO("mark_deleted all_del done", K(del3), K(kept3), K(V1), K(V2), K(V3));
    ASSERT_EQ(3, del3.count());
    ASSERT_EQ(V1, del3.at(0));
    ASSERT_EQ(V2, del3.at(1));
    ASSERT_EQ(V3, del3.at(2));
    ASSERT_EQ(0, kept3.count());
    // kept 为空时保留位点取 mark_delete_meta_scn + 1，避免位点停留在旧值导致后续轮次回退。
    share::SCN kept_meta_scn;
    OK(task.calc_kept_meta_scn_(param3, del3, kept3, kept_meta_scn));
    ASSERT_EQ(share::SCN::plus(V3, 1), kept_meta_scn);
    OK(big_ckpt_map.destroy());
  }

  // cleanup synthetic versions
  const share::SCN versions[] = {V1, V2, V3};
  for (int64_t i = 0; i < 3; ++i) {
    ObSSMetaTabletKey k;
    k.ls_id_ = ls_id;
    k.tablet_id_ = tablet_id;
    k.reorganization_scn_ = reorgan;
    k.meta_scn_ = versions[i];
    OK(delete_tablet_meta_row(*meta_svr, k));
  }
  OK(ckpt_map.destroy());
  LOG_INFO("[TEST CASE END] rule3_max_replayed_scn_vs_ls_ss_checkpoint_scn");
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  char buf[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
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
