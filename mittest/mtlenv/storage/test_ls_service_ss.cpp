// owner: cxf262476
// owner group: transaction

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGETEST
#ifdef OB_BUILD_SHARED_STORAGE

#define protected public
#define private public
#include "src/storage/slog/ob_storage_log_item.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "logservice/ob_log_service.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/ss_garbage_collector/ob_ss_gc_service.h"
#include "storage/test_tablet_helper.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
using namespace common;
using namespace rpc;
using namespace rpc::frame;
using namespace share;
namespace storage
{
// Test class for shared storage mode
class TestLSServiceSS : public ::testing::Test
{
public:
  class EmptySharedMetaConnectionPool final
      : public common::sqlclient::ObMySQLConnectionPool
  {
  public:
    EmptySharedMetaConnectionPool() = default;
    virtual ~EmptySharedMetaConnectionPool() = default;
    int acquire(
        const uint64_t tenant_id,
        common::sqlclient::ObISQLConnection *&conn,
        common::ObISQLClient *client_addr,
        const int32_t group_id) override
    {
      int ret = OB_NOT_INIT;
      conn = nullptr;
      UNUSEDx(tenant_id, client_addr, group_id);
      if (fail_next_acquire_) {
        // 仅当前测试线程的下一次 shared-meta 查询模拟记录不存在。
        fail_next_acquire_ = false;
        ret = OB_ENTRY_NOT_EXIST;
      }
      return ret;
    }

    static void set_fail_next_acquire(const bool fail)
    {
      fail_next_acquire_ = fail;
    }

  private:
    static thread_local bool fail_next_acquire_;
  };

  class EmptySharedMetaAcquireGuard final
  {
  public:
    EmptySharedMetaAcquireGuard()
    {
      EmptySharedMetaConnectionPool::set_fail_next_acquire(true);
    }
    virtual ~EmptySharedMetaAcquireGuard()
    {
      EmptySharedMetaConnectionPool::set_fail_next_acquire(false);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(EmptySharedMetaAcquireGuard);
  };

  TestLSServiceSS() = default;
  virtual ~TestLSServiceSS() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }

private:
  int init_ls_before_palf(ObLS &ls, const ObLSID &ls_id)
  {
    int ret = OB_SUCCESS;
    ObCreateLSArg arg;
    ObLSService *ls_service = MTL(ObLSService *);
    if (OB_FAIL(gen_create_ls_arg(MTL_ID(), ls_id, arg))) {
    } else {
      ret = ls.init(ls_id, MTL_ID(), ObMigrationStatus::OB_MIGRATION_STATUS_NONE,
          ls_service->get_restore_status_by_tenant_role_(arg.get_tenant_info().get_tenant_role()),
          arg.get_create_scn(), arg.get_major_mv_merge_info(),
          ObLSStoreType::OB_LS_STORE_NORMAL, arg.get_replica_type(),
          ls_service->rs_reporter_, arg.get_data_version());
    }
    return ret;
  }

  // 调用方持有 tablet_study_info_lock_。
  static int get_tablet_study_info_without_lock(
      ObSSGCService &gc_service,
      const ObSSGCTablet &gc_tablet,
      ObSSGCTabletStudyInfo &study_info,
      int64_t &ls_epoch)
  {
    int ret = OB_SUCCESS;
    ObSSGCService::LSStudyInfoCtx *study_ctx = nullptr;
    const ObSSGCService::TabletStudyKey study_key(
        gc_tablet.tablet_id_, gc_tablet.transfer_scn_);
    if (OB_FAIL(gc_service.ls_study_info_ctx_map_.get_refactored(
            gc_tablet.ls_id_, study_ctx))) {
    } else if (OB_ISNULL(study_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls study info context is null", KR(ret), K(gc_tablet));
    } else if (OB_FAIL(study_ctx->get_tablet_study_info_map().get_refactored(
                   study_key, study_info))) {
    } else {
      ls_epoch = study_ctx->get_ls_epoch();
    }
    return ret;
  }

  static EmptySharedMetaConnectionPool empty_shared_meta_connection_pool_;
  common::ObArenaAllocator allocator_;
};

thread_local bool
TestLSServiceSS::EmptySharedMetaConnectionPool::fail_next_acquire_ = false;

TestLSServiceSS::EmptySharedMetaConnectionPool
    TestLSServiceSS::empty_shared_meta_connection_pool_;

void TestLSServiceSS::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  ASSERT_EQ(OB_SUCCESS,
      MockTenantModuleEnv::get_instance().sql_proxy_.init(
          &empty_shared_meta_connection_pool_));
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
}

void TestLSServiceSS::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestLSServiceSS, test_failed_ls_create_keeps_existing_tx_mgr)
{
  const ObLSID ls_id(270);
  ObLS old_ls;
  ObLS failed_ls;
  ObLS new_ls;
  transaction::ObTxCtxMgr &tx_ctx_mgr = MTL(transaction::ObTransService *)->get_tx_ctx_mgr();
  ASSERT_EQ(OB_SUCCESS, init_ls_before_palf(old_ls, ls_id));
  transaction::ObLSTxCtxMgr *old_mgr = old_ls.get_tx_svr()->mgr_;
  ASSERT_NE(nullptr, old_mgr);

  // A has registered its mgr but has not created Palf or entered the LS map.
  // B fails during tx mgr registration; both init rollback and destruction run.
  ASSERT_EQ(OB_ENTRY_EXIST, init_ls_before_palf(failed_ls, ls_id));
  EXPECT_EQ(nullptr, failed_ls.get_tx_svr()->get_ls_tx_ctx_mgr());
  failed_ls.destroy();

  // Check the failed registration before ObLS runs its outer rollback.
  failed_ls.ls_meta_.ls_id_ = ls_id;
  transaction::ObTxPalfParam param(failed_ls.get_log_handler(), &failed_ls.dup_table_ls_handler_);
  ASSERT_EQ(OB_ENTRY_EXIST,
      MTL(transaction::ObTransService *)->create_ls(ls_id, failed_ls, &param, nullptr));
  EXPECT_EQ(nullptr, failed_ls.get_tx_svr()->get_ls_tx_ctx_mgr());
  failed_ls.destroy();
  transaction::ObLSTxCtxMgr *current_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, tx_ctx_mgr.get_ls_tx_ctx_mgr(ls_id, current_mgr));
  EXPECT_EQ(old_mgr, current_mgr);
  EXPECT_FALSE(current_mgr->is_stopped());
  EXPECT_EQ(old_ls.get_log_handler(), current_mgr->log_adapter_def_.log_handler_);
  ASSERT_EQ(OB_SUCCESS, tx_ctx_mgr.revert_ls_tx_ctx_mgr(current_mgr));

  // C cannot register until A removes its own mgr.
  {
    ObLS blocked_ls;
    ASSERT_EQ(OB_ENTRY_EXIST, init_ls_before_palf(blocked_ls, ls_id));
    EXPECT_EQ(nullptr, blocked_ls.get_tx_svr()->get_ls_tx_ctx_mgr());
  }
  old_ls.destroy();
  EXPECT_EQ(nullptr, old_ls.get_tx_svr()->get_ls_tx_ctx_mgr());
  current_mgr = nullptr;
  EXPECT_EQ(OB_PARTITION_NOT_EXIST, tx_ctx_mgr.get_ls_tx_ctx_mgr(ls_id, current_mgr));
  ASSERT_EQ(OB_SUCCESS, init_ls_before_palf(new_ls, ls_id));
  old_ls.destroy();
  failed_ls.destroy();
  ASSERT_EQ(OB_SUCCESS, tx_ctx_mgr.get_ls_tx_ctx_mgr(ls_id, current_mgr));
  EXPECT_EQ(new_ls.get_tx_svr()->get_ls_tx_ctx_mgr(), current_mgr);
  EXPECT_FALSE(current_mgr->is_stopped());
  EXPECT_EQ(new_ls.get_log_handler(), current_mgr->log_adapter_def_.log_handler_);
  ASSERT_EQ(OB_SUCCESS, tx_ctx_mgr.revert_ls_tx_ctx_mgr(current_mgr));
  new_ls.destroy();
}

TEST_F(TestLSServiceSS, test_destroy_ls_after_tx_mgr_registration)
{
  const ObLSID ls_id(272);
  ObLS partial_ls;
  transaction::ObTransService *tx_service = MTL(transaction::ObTransService *);
  transaction::ObTxCtxMgr &tx_ctx_mgr = tx_service->get_tx_ctx_mgr();
  partial_ls.ls_meta_.ls_id_ = ls_id;
  transaction::ObTxPalfParam param(partial_ls.get_log_handler(), &partial_ls.dup_table_ls_handler_);
  ASSERT_EQ(OB_SUCCESS, tx_service->create_ls(ls_id, partial_ls, &param, nullptr));
  ASSERT_FALSE(partial_ls.get_tx_table()->is_inited_);
  transaction::ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, tx_ctx_mgr.get_ls_tx_ctx_mgr(ls_id, mgr));

  // Cleanup must work before the tx table has acquired a mgr reference.
  partial_ls.destroy();
  EXPECT_EQ(nullptr, partial_ls.get_tx_svr()->get_ls_tx_ctx_mgr());
  partial_ls.destroy();
  EXPECT_TRUE(mgr->is_stopped());
  EXPECT_EQ(nullptr, mgr->log_adapter_def_.log_handler_);
  EXPECT_EQ(1, mgr->get_ref());
  transaction::ObLSTxCtxMgr *current_mgr = nullptr;
  EXPECT_EQ(OB_PARTITION_NOT_EXIST, tx_ctx_mgr.get_ls_tx_ctx_mgr(ls_id, current_mgr));
  ASSERT_EQ(OB_SUCCESS, tx_ctx_mgr.revert_ls_tx_ctx_mgr(mgr));
}

TEST_F(TestLSServiceSS, test_remove_ls_parameter_merge_ss)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  int64_t cnt = 0;
  ObCreateLSArg arg;
  ObLSService* ls_svr = MTL(ObLSService*);
  bool exist = false;
  bool waiting = false;
  ObLSID id_200(200);
  ObLSID id_201(201);
  ObLSHandle handle;
  ObLS *ls = nullptr;

  LOG_INFO("TestLSServiceSS::test_remove_ls_parameter_merge_ss");
  
  // Test purpose: Verify that remove_ls_ function signature works correctly
  // in shared storage mode, where delete_ls uses update_tenant_ls_item instead of writing slog
  
  // 1. Test with remove_from_disk = true
  LOG_INFO("TestLSServiceSS::test_remove_ls_parameter_merge_ss: test remove_from_disk = true");
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_200, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_200, exist));
  ASSERT_TRUE(exist);
  
  // Use public remove_ls interface which internally calls remove_ls_ with remove_from_disk = true
  // In shared storage mode, this will call update_tenant_ls_item instead of writing slog
  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_200));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_200, exist));
  ASSERT_FALSE(exist);
  cnt = 0;
  while (cnt++ < 20) {
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_200, waiting));
    if (waiting) {
      ::sleep(1);
    } else {
      break;
    }
  }
  
  // 2. Test direct call to remove_ls_ with remove_from_disk = true
  LOG_INFO("TestLSServiceSS::test_remove_ls_parameter_merge_ss: test direct call with remove_from_disk = true");
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_201, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(id_201, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  
  // Prepare LS for removal (similar to safe_remove_ls_)
  ASSERT_EQ(OB_SUCCESS, ls->offline(true));
  ASSERT_EQ(OB_SUCCESS, ls->stop());
  ls->wait();
  
  ObLSLockGuard lock_ls(ls);
  ASSERT_EQ(OB_SUCCESS, ls->set_remove_state(true));
  ASSERT_EQ(OB_SUCCESS, ls->prepare_for_safe_destroy());
  
  // In shared storage mode, remove_ls_ will call write_tablet_id_set_to_pending_free
  // and then use update_tenant_ls_item instead of writing slog
  ls_svr->remove_ls_(ls, true/*remove_from_disk*/);
  lock_ls.~ObLSLockGuard();
  handle.reset();
  
  // Verify LS is removed
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_201, exist));
  ASSERT_FALSE(exist);
  cnt = 0;
  while (cnt++ < 20) {
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_201, waiting));
    if (waiting) {
      ::sleep(1);
    } else {
      break;
    }
  }
  
  LOG_INFO("TestLSServiceSS::test_remove_ls_parameter_merge_ss: parameter merge verified in shared storage mode");
}

TEST_F(TestLSServiceSS, test_remove_ls_retry_after_write_slog_ss)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  int64_t cnt = 0;
  ObCreateLSArg arg;
  ObLSService* ls_svr = MTL(ObLSService*);
  bool exist = false;
  bool waiting = false;
  ObLSID id_202(202);
  ObLSHandle handle;
  ObLS *ls = nullptr;

  LOG_INFO("TestLSServiceSS::test_remove_ls_retry_after_write_slog_ss");
  
  // Test scenario: In shared storage mode, verify that remove_ls_ handles
  // update_tenant_ls_item correctly and retry mechanism works
  
  // 1. Create a LS for testing
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_202, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(id_202, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  
  // 2. Prepare LS for removal
  ASSERT_EQ(OB_SUCCESS, ls->offline(true));
  ASSERT_EQ(OB_SUCCESS, ls->stop());
  ls->wait();
  
  ObLSLockGuard lock_ls(ls);
  ASSERT_EQ(OB_SUCCESS, ls->set_remove_state(true));
  
  // 3. In shared storage mode, write_tablet_id_set_to_pending_free is called first
  ASSERT_EQ(OB_SUCCESS, ls->write_tablet_id_set_to_pending_free());
  ASSERT_EQ(OB_SUCCESS, ls->prepare_for_safe_destroy());
  
  // 4. Manually update tenant ls item to simulate the scenario where
  // update_tenant_ls_item is called successfully but subsequent steps might fail
  const share::ObLSID &ls_id = ls->get_ls_id();
  ObTenantStorageMetaService *meta_service = MTL(ObTenantStorageMetaService*);
  ASSERT_NE(nullptr, meta_service);
  // In shared storage mode, delete_ls calls update_tenant_ls_item
  ASSERT_EQ(OB_SUCCESS, meta_service->delete_ls(ls_id, ls->get_ls_epoch()));
  ASSERT_EQ(OB_SUCCESS, TENANT_STORAGE_META_SERVICE.delete_ls_item(ls_id, ls->get_ls_epoch()));
  
  // Verify LS still exists in map (item updated but remove_ls_ not completed yet)
  lock_ls.~ObLSLockGuard();
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_202, exist));
  ASSERT_TRUE(exist);
  
  // 5. Now call remove_ls_ which should handle the case where update_tenant_ls_item is already done
  // The remove_ls_ function will check and proceed with remaining steps
  ObLSLockGuard lock_ls2(ls);
  ASSERT_EQ(OB_SUCCESS, ls->write_tablet_id_set_to_pending_free());
  ASSERT_EQ(OB_SUCCESS, ls->prepare_for_safe_destroy());
  ls_svr->remove_ls_(ls, true/*remove_from_disk*/);
  lock_ls2.~ObLSLockGuard();
  handle.reset();
  
  // 6. Verify LS is removed from map
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_202, exist));
  ASSERT_FALSE(exist);
  cnt = 0;
  while (cnt++ < 20) {
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_202, waiting));
    if (waiting) {
      ::sleep(1);
    } else {
      break;
    }
  }
  
  LOG_INFO("TestLSServiceSS::test_remove_ls_retry_after_write_slog_ss: retry after update tenant ls item verified");
}

TEST_F(TestLSServiceSS, test_replay_remove_ls_idempotent_ss)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  int64_t cnt = 0;
  ObCreateLSArg arg;
  ObLSService* ls_svr = MTL(ObLSService*);
  bool exist = false;
  bool waiting = false;
  ObLSID id_203(203);
  ObLSHandle handle;
  ObLS *ls = nullptr;

  LOG_INFO("TestLSServiceSS::test_replay_remove_ls_idempotent_ss");
  
  // Test scenario: In shared storage mode, simulate restart and replay remove ls slog multiple times
  // The replay should be idempotent - safe to replay the same remove operation multiple times
  
  // 1. Create a LS
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_203, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_203, exist));
  ASSERT_TRUE(exist);
  
  // 2. Get LS and set it to remove state (simulating normal remove process)
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(id_203, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  
  ObLSLockGuard lock_ls(ls);
  // Set remove state without writing slog (simulating replay scenario)
  // In shared storage mode, replay_remove_ls also sets remove state
  ASSERT_EQ(OB_SUCCESS, ls->set_remove_state(false /*write_slog*/));
  lock_ls.~ObLSLockGuard();
  handle.reset();
  
  // 3. First replay remove ls (normal case)
  ASSERT_EQ(OB_SUCCESS, ls_svr->replay_remove_ls(id_203));
  
  // 4. Replay the same remove ls again (simulating duplicate replay after restart)
  // This should be safe - replay_remove_ls should handle the case where LS doesn't exist
  ASSERT_EQ(OB_SUCCESS, ls_svr->replay_remove_ls(id_203));
  
  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_203)); 
  // Verify LS still doesn't exist (idempotent)
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_203, exist));
  ASSERT_FALSE(exist);
  
  cnt = 0;
  while (cnt++ < 20) {
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_203, waiting));
    if (waiting) {
      ::sleep(1);
    } else {
      break;
    }
  }
  
  LOG_INFO("TestLSServiceSS::test_replay_remove_ls_idempotent_ss: idempotent replay verified in shared storage mode");
  
  // 6. Test replay remove for non-existent LS (edge case)
  ObLSID id_204(204);
  ASSERT_EQ(OB_SUCCESS, ls_svr->replay_remove_ls(id_204));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_204, exist));
  ASSERT_FALSE(exist);
  
  LOG_INFO("TestLSServiceSS::test_replay_remove_ls_idempotent_ss: all scenarios verified in shared storage mode");
}

TEST_F(TestLSServiceSS, test_same_ls_rebuild_resets_study_info_by_ls_epoch)
{
  const ObLSID ls_id(206);
  const ObTabletID tablet_id(200001);
  const ObSSGCTabletStudyInfo old_study_info(SCN::max_scn(), 20);
  ObCreateLSArg arg;
  ObLSService *ls_service = MTL(ObLSService *);
  ObSSGCService *gc_service = MTL(ObSSGCService *);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  share::schema::ObTableSchema table_schema;
  ObSSGCTablet old_gc_tablet;
  ObSSGCTablet new_gc_tablet;
  ObSSGCTabletStudyInfo new_study_info;
  ScanAllVersionTabletsOp::GetMinTabletRelyInfo study_op(
      new_study_info.min_dep_meta_scn_, new_study_info.min_major_snapshot_);
  ObSSGCTabletStudyInfo saved_study_info;
  int64_t saved_ls_epoch = -1;
  int64_t old_ls_epoch = -1;
  int64_t new_ls_epoch = -1;
  int64_t retry_count = 0;
  bool waiting = true;

  ASSERT_NE(nullptr, ls_service);
  ASSERT_NE(nullptr, gc_service);
  ASSERT_NE(nullptr, t3m);
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(MTL_ID(), ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, 12345));
  ASSERT_EQ(OB_SUCCESS, ls_service->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS,
      ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls_handle.get_ls());
  old_ls_epoch = ls_handle.get_ls()->get_ls_epoch();
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(
      ls_handle, tablet_id, table_schema, allocator_));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  old_gc_tablet = ObSSGCTablet(
      ls_id, tablet_id, tablet_handle.get_obj()->get_reorganization_scn());
  tablet_handle.reset();
  ASSERT_EQ(OB_SUCCESS,
      gc_service->check_and_update_local_tablet_study_info(
          old_gc_tablet, old_study_info, old_ls_epoch));
  ASSERT_EQ(OB_SUCCESS, ls_service->remove_ls(ls_id));
  ASSERT_EQ(OB_LS_WAITING_SAFE_DESTROY, ls_service->create_ls(arg));
  ls_handle.reset();
  while (retry_count++ < 20 && waiting) {
    ASSERT_EQ(OB_SUCCESS, ls_service->check_ls_waiting_safe_destroy(ls_id, waiting));
    if (waiting) {
      ::sleep(1);
    }
  }
  ASSERT_FALSE(waiting);

  ASSERT_EQ(OB_SUCCESS, ls_service->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS,
      ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls_handle.get_ls());
  new_ls_epoch = ls_handle.get_ls()->get_ls_epoch();
  ASSERT_GT(new_ls_epoch, old_ls_epoch);
  {
    lib::ObMutexGuard guard(gc_service->tablet_study_info_lock_);
    ASSERT_EQ(OB_SUCCESS, guard.get_ret());
    ASSERT_EQ(OB_SUCCESS, get_tablet_study_info_without_lock(
        *gc_service, old_gc_tablet, saved_study_info, saved_ls_epoch));
    EXPECT_EQ(old_ls_epoch, saved_ls_epoch);
    EXPECT_EQ(20, saved_study_info.min_major_snapshot_);
  }
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(
      ls_handle, tablet_id, table_schema, allocator_));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  new_gc_tablet = ObSSGCTablet(
      ls_id, tablet_id, tablet_handle.get_obj()->get_reorganization_scn());
  ASSERT_TRUE(old_gc_tablet == new_gc_tablet);
  tablet_handle.reset();
  ASSERT_EQ(OB_SUCCESS,
      t3m->scan_all_version_tablets(ObTabletMapKey(ls_id, tablet_id), study_op));
  ASSERT_FALSE(new_study_info.has_finite_rely());
  ASSERT_EQ(1, new_study_info.min_major_snapshot_);
  ASSERT_EQ(OB_SUCCESS,
      gc_service->check_and_update_local_tablet_study_info(
          new_gc_tablet, new_study_info, new_ls_epoch));
  {
    lib::ObMutexGuard guard(gc_service->tablet_study_info_lock_);
    ASSERT_EQ(OB_SUCCESS, guard.get_ret());
    ASSERT_EQ(OB_SUCCESS, get_tablet_study_info_without_lock(
        *gc_service, new_gc_tablet, saved_study_info, saved_ls_epoch));
    EXPECT_EQ(new_ls_epoch, saved_ls_epoch);
    EXPECT_EQ(1, saved_study_info.min_major_snapshot_);
  }
  ls_handle.reset();
  ASSERT_EQ(OB_SUCCESS, ls_service->remove_ls(ls_id));
}

TEST_F(TestLSServiceSS, test_cleanup_cleans_tablet_gc_study_info)
{
  const ObLSID ls_id(207);
  const ObTabletID tablet_id(200003);
  const ObSSGCTabletStudyInfo study_info(
      SCN::plus(SCN::base_scn(), 200), 20);
  const SCN create_commit_scn = SCN::plus(SCN::base_scn(), 100);
  const bool is_immediate_gc = false;
  ObCreateLSArg arg;
  ObLSService *ls_service = MTL(ObLSService *);
  ObSSGCService *gc_service = MTL(ObSSGCService *);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObSSGCTablet gc_tablet;
  ObSSGCTabletStudyInfo saved_study_info;
  int64_t saved_ls_epoch = -1;
  ObTabletResidentInfo resident_info;
  ObSEArray<ObTabletHandle, 1> deleted_tablets;
  share::schema::ObTableSchema table_schema;
  checkpoint::ObTabletGCHandler *tablet_gc_handler = nullptr;
  int resident_ret = OB_SUCCESS;
  int64_t ls_epoch = -1;
  bool ls_exist = false;

  ASSERT_NE(nullptr, ls_service);
  ASSERT_NE(nullptr, gc_service);
  ASSERT_NE(nullptr, t3m);
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(MTL_ID(), ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, 12346));
  ASSERT_EQ(OB_SUCCESS, ls_service->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS,
      ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls_handle.get_ls());
  ls_epoch = ls_handle.get_ls()->get_ls_epoch();
  tablet_gc_handler = ls_handle.get_ls()->get_tablet_gc_handler();
  ASSERT_NE(nullptr, tablet_gc_handler);
  {
    // 写锁阻止后台 tablet GC 抢先消费本用例构造的 empty shell。
    obsys::ObWLockGuard<> lock(tablet_gc_handler->wait_lock_);
    ASSERT_TRUE(lock.acquired());
    ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(
        ls_handle,
        tablet_id,
        table_schema,
        allocator_,
        ObTabletStatus::Status::DELETED,
        create_commit_scn,
        tablet_handle));
    ASSERT_NE(nullptr, tablet_handle.get_obj());
    gc_tablet = ObSSGCTablet(
        ls_id, tablet_id, tablet_handle.get_obj()->get_reorganization_scn());
    ASSERT_EQ(OB_SUCCESS,
        gc_service->check_and_update_local_tablet_study_info(
            gc_tablet, study_info, ls_epoch));

    tablet_handle.reset();
    ASSERT_EQ(OB_SUCCESS,
        ls_handle.get_ls()->get_tablet_svr()->update_tablet_to_empty_shell(
            DATA_CURRENT_VERSION, tablet_id));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->get_tablet(
        tablet_id,
        tablet_handle,
        0,
        ObMDSGetTabletMode::READ_WITHOUT_CHECK));
    ASSERT_NE(nullptr, tablet_handle.get_obj());
    ASSERT_TRUE(tablet_handle.get_obj()->is_empty_shell());
    ASSERT_EQ(gc_tablet.transfer_scn_, tablet_handle.get_obj()->get_reorganization_scn());
    ASSERT_TRUE(tablet_handle.get_obj()->get_tablet_meta()
        .empty_shell_tablet_status_.delete_commit_scn_.is_valid());
    ASSERT_EQ(OB_SUCCESS, deleted_tablets.push_back(tablet_handle));
    {
      lib::ObMutexGuard guard(gc_service->tablet_study_info_lock_);
      ASSERT_EQ(OB_SUCCESS, guard.get_ret());
      ASSERT_EQ(OB_SUCCESS, get_tablet_study_info_without_lock(
          *gc_service, gc_tablet, saved_study_info, saved_ls_epoch));
      ASSERT_EQ(study_info.min_dep_meta_scn_,
          saved_study_info.min_dep_meta_scn_);
      ASSERT_EQ(20, saved_study_info.min_major_snapshot_);
      ASSERT_EQ(ls_epoch, saved_ls_epoch);
    }
    {
      // 仅当前线程模拟 shared meta 已不存在。
      EmptySharedMetaAcquireGuard empty_shared_meta_guard;
      ASSERT_EQ(OB_SUCCESS,
          tablet_gc_handler->gc_tablets(deleted_tablets, is_immediate_gc));
    }
  }

  ASSERT_EQ(OB_SUCCESS, ls_service->check_ls_exist(ls_id, ls_exist));
  ASSERT_TRUE(ls_exist);
  resident_ret = t3m->get_tablet_resident_info(
      ObTabletMapKey(ls_id, tablet_id), resident_info);
  EXPECT_TRUE(OB_ENTRY_NOT_EXIST == resident_ret || OB_HASH_NOT_EXIST == resident_ret);
  {
    lib::ObMutexGuard guard(gc_service->tablet_study_info_lock_);
    ASSERT_EQ(OB_SUCCESS, guard.get_ret());
    ASSERT_EQ(OB_SUCCESS, get_tablet_study_info_without_lock(
        *gc_service, gc_tablet, saved_study_info, saved_ls_epoch));
    ASSERT_EQ(study_info.min_dep_meta_scn_,
        saved_study_info.min_dep_meta_scn_);
    ASSERT_EQ(20, saved_study_info.min_major_snapshot_);
    ASSERT_EQ(ls_epoch, saved_ls_epoch);
  }
  ASSERT_EQ(OB_SUCCESS, gc_service->cleanup_stale_local_tablet_study_info());
  {
    ObSSGCService::LSStudyInfoCtx *study_ctx = nullptr;
    lib::ObMutexGuard guard(gc_service->tablet_study_info_lock_);
    ASSERT_EQ(OB_SUCCESS, guard.get_ret());
    EXPECT_EQ(OB_HASH_NOT_EXIST, get_tablet_study_info_without_lock(
        *gc_service, gc_tablet, saved_study_info, saved_ls_epoch));
    EXPECT_EQ(OB_HASH_NOT_EXIST,
        gc_service->ls_study_info_ctx_map_.get_refactored(
            ls_id, study_ctx));
  }
  deleted_tablets.reset();
  tablet_handle.reset();
  ls_handle.reset();
  ASSERT_EQ(OB_SUCCESS, ls_service->remove_ls(ls_id));
}

#ifdef OB_BUILD_SHARED_LOG_SERVICE
TEST_F(TestLSServiceSS, test_gc_stale_committed_ls_with_logservice_after_replay_ss)
{
  const uint64_t tenant_id = MTL_ID();
  ObCreateLSArg arg;
  ObLSService *ls_svr = MTL(ObLSService *);
  logservice::ObLogService *log_svr = MTL(logservice::ObLogService *);
  const ObLSID ls_id(205);
  ObLSHandle handle;
  ObLS *ls = nullptr;
  ObLSMeta replay_ls_meta;
  ObArray<ObLSItem> ls_items;
  palf::PalfBaseInfo palf_base_info;
  ObTenantRole unused_tenant_role = share::PRIMARY_TENANT_ROLE;
  const bool allow_log_sync = false;
  int64_t ls_epoch = 0;
  int64_t retry_count = 0;
  bool exist = false;
  bool item_found = false;
  bool waiting = false;
  bool palf_exist = true;

  ASSERT_NE(nullptr, ls_svr);
  ASSERT_NE(nullptr, log_svr);
  ASSERT_TRUE(GCTX.is_shared_storage_mode());
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  replay_ls_meta = ls->get_ls_meta();
  replay_ls_meta.ls_persistent_state_ = ObLSPersistentState::State::LS_INIT;
  ls_epoch = ls->get_ls_epoch();
  ASSERT_TRUE(ls->is_create_committed());
  ASSERT_FALSE(ls->is_stale());

  // A stopped LogHandler is invalid even though its Palf stream still exists.
  // This generic lifecycle state must never authorize committed-LS deletion.
  ASSERT_EQ(OB_SUCCESS, ls->offline(true/*remove_from_disk*/));
  ASSERT_EQ(OB_SUCCESS, ls->stop());
  ls->wait();
  ASSERT_FALSE(ls->get_log_handler()->is_valid());
  ASSERT_FALSE(ls->is_stale());
  handle.reset();
  ASSERT_EQ(OB_SUCCESS, log_svr->check_palf_exist(ls_id, palf_exist));
  ASSERT_TRUE(palf_exist);
  ASSERT_EQ(OB_SUCCESS, ls_svr->gc_ls_after_replay_slog());
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(ls_id, exist));
  ASSERT_TRUE(exist);

  // Remove only the LogService/Palf state. Keep the LS in the map and its
  // tenant item CREATED, then replay INIT + COMMIT to build the same invariant
  // observed after startup replay: committed LS with an invalid LogHandler.
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  {
    ObLSLockGuard lock_ls(ls);
    ASSERT_EQ(OB_SUCCESS, ls->remove_ls());
  }
  ASSERT_FALSE(ls->get_log_handler()->is_valid());
  handle.reset();
  ASSERT_EQ(OB_SUCCESS, log_svr->check_palf_exist(ls_id, palf_exist));
  ASSERT_FALSE(palf_exist);
  ASSERT_EQ(OB_SUCCESS,
      TENANT_STORAGE_META_SERVICE.get_ls_items_by_status(ObLSItemStatus::CREATED, ls_items));
  for (int64_t i = 0; i < ls_items.count(); ++i) {
    if (ls_items.at(i).ls_id_ == ls_id && ls_items.at(i).epoch_ == ls_epoch) {
      item_found = true;
      break;
    }
  }
  ASSERT_TRUE(item_found);

  ASSERT_EQ(OB_SUCCESS, ls_svr->replay_create_ls(ls_epoch, replay_ls_meta));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ASSERT_TRUE(ls->get_persistent_state().is_init_state());
  ASSERT_FALSE(ls->get_log_handler()->is_valid());
  handle.reset();

  ASSERT_EQ(OB_SUCCESS, ls_svr->replay_create_ls_commit(ls_id));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ASSERT_TRUE(ls->is_create_committed());
  ASSERT_FALSE(ls->get_log_handler()->is_valid());
  ASSERT_FALSE(ls->is_stale());
  handle.reset();

  // Missing Palf plus an invalid handler is still insufficient until startup
  // load records the exact not-exist result.
  ASSERT_EQ(OB_SUCCESS, ls_svr->gc_ls_after_replay_slog());
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(ls_id, exist));
  ASSERT_TRUE(exist);

  // Shared-storage mode with local Palf must preserve the existing fail-fast
  // behavior instead of treating local log loss as a stale LibPalf stream.
  ASSERT_FALSE(GCONF.enable_logservice);
  ASSERT_EQ(OB_SUCCESS, ls_svr->gc_ls_after_replay_slog());
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(ls_id, exist));
  ASSERT_TRUE(exist);

  // MockTenantModuleEnv uses local Palf, so invoke the same load path after
  // removing Palf to produce the explicit not-exist classification.
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  palf_base_info.generate_by_default();
  ASSERT_EQ(OB_SUCCESS,
      ls->load_ls(unused_tenant_role, palf_base_info, allow_log_sync));
  ASSERT_TRUE(ls->is_stale());
  handle.reset();

  ASSERT_EQ(OB_SUCCESS, ls_svr->gc_ls_after_replay_slog());
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(ls_id, exist));
  ASSERT_FALSE(exist);
  item_found = false;
  ASSERT_EQ(OB_SUCCESS,
      TENANT_STORAGE_META_SERVICE.get_ls_items_by_status(ObLSItemStatus::DELETED, ls_items));
  for (int64_t i = 0; i < ls_items.count(); ++i) {
    if (ls_items.at(i).ls_id_ == ls_id && ls_items.at(i).epoch_ == ls_epoch) {
      item_found = true;
      break;
    }
  }
  ASSERT_TRUE(item_found);

  retry_count = 0;
  waiting = false;
  while (retry_count++ < 20) {
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(ls_id, waiting));
    if (waiting) {
      ::sleep(1);
    } else {
      break;
    }
  }
  ASSERT_FALSE(waiting);
  ASSERT_EQ(OB_SUCCESS, ls_svr->online_ls());
}
#endif

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ls_service_ss.log*");
  OB_LOGGER.set_file_name("test_ls_service_ss.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#endif // OB_BUILD_SHARED_STORAGE
