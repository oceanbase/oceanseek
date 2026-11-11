/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#include "test_shared_meta_common.h"
#include <gmock/gmock.h>
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/ddl/ob_ddl_replay_executor.h"
#include "storage/mds_table/ob_single_log_mds_ctx.h"
#include "storage/ddl/ob_ss_ddl_util.h"
#include "storage/incremental/ssmeta/ob_ss_meta_atomic_op.h"
#include "unittest/storage/init_basic_struct.h"
#include "src/logservice/libpalf/libpalf_kv_instance.h"
#include "storage/schema_utils.h"
#include "share/io/ob_io_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#define private public
#define protected public
#include "storage/high_availability/ob_ss_transfer_backfill_dag_net.h"
#include "storage/high_availability/ob_ss_transfer_backfill_utils.h"
#undef private
#undef protected

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace unittest
{

// 仅在当前测试线程接管 PALF adapter 读取，保留 MetaService 的真实解析和校验逻辑。
class MockLsCkptPalfAdapter
{
public:
  MockLsCkptPalfAdapter() : value_() {}
  virtual ~MockLsCkptPalfAdapter() {}

  MOCK_METHOD2(get, int(const common::ObString &, common::ObStringBuffer &));

  int fill_value(
      const common::ObString &key,
      common::ObStringBuffer &value)
  {
    int ret = value.append(value_);
    UNUSED(key);
    return ret;
  }

  void set_value(const common::ObString &value) { value_ = value; }
  static MockLsCkptPalfAdapter *get_active_mock() { return active_mock_; }
  static void set_active_mock(MockLsCkptPalfAdapter *mock) { active_mock_ = mock; }

private:
  static __thread MockLsCkptPalfAdapter *active_mock_;
  common::ObString value_;

  DISALLOW_COPY_AND_ASSIGN(MockLsCkptPalfAdapter);
};

__thread MockLsCkptPalfAdapter *MockLsCkptPalfAdapter::active_mock_ = nullptr;

} // namespace unittest
namespace sslog
{

// 沿用本测试的可执行文件符号覆盖方式；未激活脚本时继续委托真实 PALF KV。
int ObSSLogKVPalfAdapter::get(
    const common::ObString &key,
    common::ObStringBuffer &value_buffer)
{
  int ret = OB_SUCCESS;
  unittest::MockLsCkptPalfAdapter *mock = unittest::MockLsCkptPalfAdapter::get_active_mock();
  if (!is_inited()) {
    ret = OB_NOT_INIT;
  } else if (OB_NOT_NULL(mock) && common::is_sys_tenant(MTL_ID())) {
    ret = mock->get(key, value_buffer);
  } else {
    ret = libpalf::LibPalfKVInstance::get_instance().get(
        cluster_id_str_.string(), tenant_id_str_.string(), key, value_buffer);
  }
  return ret;
}

} // namespace sslog
} // namespace oceanbase


int64_t lease_epoch = 1;
bool mock_sswriter_enabled = true;
thread_local int64_t check_lease_call_count = 0;
thread_local bool mock_is_sswriter = true;
thread_local bool mock_sswriter_addr_valid = true;
static thread_local bool mock_replay_writer = false;
static thread_local int64_t ddl_lease_reject_at = 0;
static thread_local int64_t ddl_epoch_change_at = 0;
static thread_local int ddl_lease_error = OB_SUCCESS;
static thread_local ObSSWriterType last_lease_type = ObSSWriterType::INVALID;

int ObSSWriterService::check_lease(
    const ObSSWriterKey &key,
    bool &is_sswriter,
    int64_t &epoch)
{
  ++check_lease_call_count;
  last_lease_type = key.get_type();
  is_sswriter = ATOMIC_LOAD(&mock_sswriter_enabled) && mock_is_sswriter;
  if (ddl_lease_reject_at > 0 && check_lease_call_count >= ddl_lease_reject_at) {
    is_sswriter = false;
  }
  epoch = ATOMIC_LOAD(&lease_epoch);
  if (ddl_epoch_change_at > 0 && check_lease_call_count >= ddl_epoch_change_at) {
    ++epoch;
  }
  return ddl_lease_error;
}

int ObSSWriterService::check_lease_by_palf_role(
    const ObSSWriterKey &key,
    bool &is_sswriter,
    int64_t &epoch)
{
  ++check_lease_call_count;
  last_lease_type = key.get_type();
  is_sswriter = (ATOMIC_LOAD(&mock_sswriter_enabled) || mock_replay_writer) && mock_is_sswriter;
  if (ddl_lease_reject_at > 0 && check_lease_call_count >= ddl_lease_reject_at) {
    is_sswriter = false;
  }
  epoch = ATOMIC_LOAD(&lease_epoch);
  if (ddl_epoch_change_at > 0 && check_lease_call_count >= ddl_epoch_change_at) {
    ++epoch;
  }
  return ddl_lease_error;
}

class DDLFinishLeaseGuard
{
public:
  DDLFinishLeaseGuard()
    : saved_sswriter_enabled_(ATOMIC_LOAD(&mock_sswriter_enabled)),
      saved_replay_writer_(mock_replay_writer) {}
  ~DDLFinishLeaseGuard()
  {
    mock_is_sswriter = true;
    ddl_lease_reject_at = 0;
    ddl_epoch_change_at = 0;
    ddl_lease_error = OB_SUCCESS;
    ATOMIC_STORE(&mock_sswriter_enabled, saved_sswriter_enabled_);
    mock_replay_writer = saved_replay_writer_;
  }
private:
  bool saved_sswriter_enabled_;
  bool saved_replay_writer_;
};

class DDLFinishTabletGuard
{
public:
  DDLFinishTabletGuard(ObLSTabletService &tablet_svr, const ObIArray<ObTabletID> &tablet_ids)
    : tablet_svr_(tablet_svr), tablet_ids_(tablet_ids)
  {}
  ~DDLFinishTabletGuard()
  {
    // Remove this case's local tablets even on assertion failure to avoid affecting later cases.
    EXPECT_EQ(OB_SUCCESS, tablet_svr_.remove_tablets(tablet_ids_));
  }
private:
  ObLSTabletService &tablet_svr_;
  const ObIArray<ObTabletID> &tablet_ids_;
};

int ObSSWriterService::get_sswriter_addr(
    const ObSSWriterKey &,
    ObSSWriterAddr &sswriter_addr,
    const int64_t)
{
  if (mock_sswriter_addr_valid) {
    sswriter_addr.addr_ = GCTX.self_addr();
    sswriter_addr.epoch_ = ATOMIC_LOAD(&lease_epoch);
  } else {
    sswriter_addr.reset();
  }
  return OB_SUCCESS;
}

namespace oceanbase
{
char *shared_storage_info = nullptr;
bool new_tenant_created = false;
namespace unittest
{

class SetWaitGCBodyFunction final : public ObIMetaFunction
{
public:
  SetWaitGCBodyFunction()
    : ObIMetaFunction(ObIMetaFunction::MetaFuncType::FUNC_TYPE_TEST)
  {}
  virtual ~SetWaitGCBodyFunction() = default;

  virtual int operator()(ObSSLSMeta &ls_meta) override
  {
    bool updated = false;
    return ls_meta.update_for_ss_gc_state(
        logservice::LSGCState::WAIT_GC, share::SCN::invalid_scn(), updated);
  }
};

static int upload_tablet_meta_for_test(
    ObSSLsCkptWriter &writer,
    const ObSSMetaTabletKey &tablet_key,
    ObSSLsCkptData &ckpt_data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(writer.upload_tablet_meta(tablet_key))) {
    LOG_WARN("upload tablet meta failed", KR(ret), K(tablet_key));
  } else {
    ObSSMetaTabletKey tablet_key_copy(tablet_key);
    const blocksstable::MacroBlockId tablet_macro_id =
        tablet_key_copy.convert_to_macro_id(false /*is_in_table*/);
    if (OB_FAIL(ckpt_data.add_tablet_meta_key(tablet_macro_id))) {
      LOG_WARN("add tablet meta to checkpoint data failed",
               KR(ret), K(tablet_key), K(tablet_macro_id));
    }
  }
  return ret;
}

TestRunCtx TestSharedMetaService::run_ctx_;

void TestSharedMetaService::TearDownTestCase()
{
  ResidualDataCleanerHelper::clean_in_mock_env();
  ObSimpleClusterTestBase::TearDownTestCase();
}

TEST_F(TestSharedMetaService, select_initial_ss_checkpoint_scn_by_create_mode)
{
  const SCN local_checkpoint_scn = SCN::plus(SCN::base_scn(), 200);
  const SCN ss_rec_scn = SCN::plus(SCN::base_scn(), 100);
  SCN initial_checkpoint_scn;

  ASSERT_EQ(OB_SUCCESS, ObSSMetaService::gen_initial_ss_checkpoint_scn_(
      ObSSLSCreateMode::NORMAL_CREATE,
      local_checkpoint_scn,
      SCN::invalid_scn(),
      initial_checkpoint_scn));
  ASSERT_EQ(local_checkpoint_scn, initial_checkpoint_scn);

  ASSERT_EQ(OB_SUCCESS, ObSSMetaService::gen_initial_ss_checkpoint_scn_(
      ObSSLSCreateMode::BOOTSTRAP_CREATE,
      local_checkpoint_scn,
      ss_rec_scn,
      initial_checkpoint_scn));
  ASSERT_EQ(ss_rec_scn, initial_checkpoint_scn);

  ASSERT_EQ(OB_SUCCESS, ObSSMetaService::gen_initial_ss_checkpoint_scn_(
      ObSSLSCreateMode::BOOTSTRAP_CREATE,
      local_checkpoint_scn,
      SCN::min_scn(),
      initial_checkpoint_scn));
  ASSERT_EQ(SCN::base_scn(), initial_checkpoint_scn);

  ASSERT_EQ(OB_ERR_UNEXPECTED, ObSSMetaService::gen_initial_ss_checkpoint_scn_(
      ObSSLSCreateMode::BOOTSTRAP_CREATE,
      local_checkpoint_scn,
      SCN::plus(local_checkpoint_scn, 1),
      initial_checkpoint_scn));
  ASSERT_FALSE(initial_checkpoint_scn.is_valid());

  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMetaService::gen_initial_ss_checkpoint_scn_(
      static_cast<ObSSLSCreateMode>(-1),
      local_checkpoint_scn,
      ss_rec_scn,
      initial_checkpoint_scn));
  ASSERT_FALSE(initial_checkpoint_scn.is_valid());
}

TEST_F(TestSharedMetaService, create_ls_basic)
{
  LOG_INFO("[TEST CASE BEGIN] create_ls_basic");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));
  int ret = OB_SUCCESS;

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  const ObLSID ls_id(1050);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));

  // 1. create LS locally via ObLSService
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));

  // 2. verify LS exists locally
  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ASSERT_EQ(ls_id, ls->get_ls_id());

  // Make the local MDS tablet observably different from a first-created shared
  // tablet. The inner-tablet create path must rebuild it through
  // acquire_first_created_ss_tablet_(), which resets clog_checkpoint_scn rather
  // than serializing this local value directly.
  ObTabletHandle local_mds_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(LS_MDS_TABLET, local_mds_tablet_handle));
  ASSERT_TRUE(local_mds_tablet_handle.is_valid());
  ObTablet *local_mds_tablet = local_mds_tablet_handle.get_obj();
  ASSERT_NE(nullptr, local_mds_tablet);
  const SCN original_mds_checkpoint_scn = local_mds_tablet->get_clog_checkpoint_scn();
  const SCN advanced_mds_checkpoint_scn = SCN::plus(original_mds_checkpoint_scn, 1);
  local_mds_tablet->tablet_meta_.clog_checkpoint_scn_ = advanced_mds_checkpoint_scn;
  bool need_restore_mds_checkpoint = true;
  auto restore_mds_checkpoint_guard = MAKE_SCOPE(
      need_restore_mds_checkpoint, [&](bool &need_restore) {
        if (need_restore) {
          local_mds_tablet->tablet_meta_.clog_checkpoint_scn_ = original_mds_checkpoint_scn;
        }
      });

  // 3. call ObSSMetaService::create_ls to persist LS meta to shared storage
  ASSERT_EQ(OB_SUCCESS,
            meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));
  local_mds_tablet->tablet_meta_.clog_checkpoint_scn_ = original_mds_checkpoint_scn;
  need_restore_mds_checkpoint = false;

  // 4. verify ls_meta is readable from shared storage
  ObSSLSMeta ls_meta;
  ASSERT_FALSE(ls_meta.is_valid());
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(ls_id, ls_meta));
  ASSERT_TRUE(ls_meta.is_valid());
  ASSERT_EQ(ls->get_clog_checkpoint_scn(), ls_meta.get_ss_checkpoint_scn());
  ASSERT_EQ(ls->get_clog_base_lsn(), ls_meta.get_ss_base_lsn());
  LOG_INFO("create_ls_basic: get_ls_meta succeed", K(ls_id), K(ls_meta));

  ObArenaAllocator shared_mds_allocator("SharedMds");
  ObTabletHandle shared_mds_tablet_handle;
  SCN shared_mds_meta_scn;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(
      ls_id,
      LS_MDS_TABLET,
      local_mds_tablet->get_reorganization_scn(),
      shared_mds_allocator,
      shared_mds_tablet_handle,
      shared_mds_meta_scn));
  ASSERT_TRUE(shared_mds_tablet_handle.is_valid());
  ASSERT_TRUE(shared_mds_meta_scn.is_valid());
  ASSERT_EQ(ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN,
            shared_mds_tablet_handle.get_obj()->get_clog_checkpoint_scn());
  ASSERT_NE(advanced_mds_checkpoint_scn,
            shared_mds_tablet_handle.get_obj()->get_clog_checkpoint_scn());

  // 5. verify create result resolution for existing LS and all inner tablets
  ASSERT_EQ(OB_SUCCESS,
            meta_svr->resolve_create_ret_(OB_ENTRY_EXIST,
                                          ObSSMetaTypeEnum::LS_META,
                                          ls_id,
                                          ObTabletID(),
                                          share::SCN::invalid_scn()));
  ASSERT_EQ(OB_SUCCESS,
            meta_svr->resolve_create_ret_(OB_ERR_PRIMARY_KEY_DUPLICATE,
                                          ObSSMetaTypeEnum::LS_META,
                                          ls_id,
                                          ObTabletID(),
                                          share::SCN::invalid_scn()));
  const ObLSID missing_ls_id(1051);
  ASSERT_EQ(OB_NEED_RETRY,
            meta_svr->resolve_create_ret_(OB_ERR_PRIMARY_KEY_DUPLICATE,
                                          ObSSMetaTypeEnum::LS_META,
                                          missing_ls_id,
                                          ObTabletID(),
                                          share::SCN::invalid_scn()));

  static constexpr int64_t INNER_TABLET_CNT = 5;
  const ObTabletID inner_tablet_ids[INNER_TABLET_CNT] = {
      LS_TX_CTX_TABLET,
      LS_TX_DATA_TABLET,
      LS_LOCK_TABLET,
      LS_REORG_INFO_TABLET,
      LS_MDS_TABLET};
  for (int64_t i = 0; i < INNER_TABLET_CNT; ++i) {
    ObTabletHandle inner_tablet_handle;
    ASSERT_EQ(OB_SUCCESS, ls->get_tablet(inner_tablet_ids[i], inner_tablet_handle));
    ASSERT_TRUE(inner_tablet_handle.is_valid());
    const ObTablet *inner_tablet = inner_tablet_handle.get_obj();
    ASSERT_NE(nullptr, inner_tablet);
    ASSERT_EQ(OB_SUCCESS,
              meta_svr->resolve_create_ret_(OB_ERR_PRIMARY_KEY_DUPLICATE,
                                            ObSSMetaTypeEnum::TABLET_META,
                                            ls_id,
                                            inner_tablet_ids[i],
                                            inner_tablet->get_reorganization_scn()));
  }

  // A duplicate without its target object must be retried rather than overwritten.
  const ObTabletID missing_tablet_id(
      ObTabletID::MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID - 1);
  ASSERT_EQ(OB_NEED_RETRY,
            meta_svr->resolve_create_ret_(OB_ERR_PRIMARY_KEY_DUPLICATE,
                                          ObSSMetaTypeEnum::TABLET_META,
                                          ls_id,
                                          missing_tablet_id,
                                          share::SCN::min_scn()));

  // 6. idempotent: create_ls again should succeed (already exists, skipped)
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));

  // cleanup
  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
  LOG_INFO("[TEST CASE END] create_ls_basic");
}

TEST_F(TestSharedMetaService, create_ls_bootstrap_mode_uses_ss_rec_scn)
{
  LOG_INFO("[TEST CASE BEGIN] create_ls_bootstrap_mode_uses_ss_rec_scn");
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, ls_svr);
  ASSERT_NE(nullptr, meta_svr);

  const ObLSID ls_id(1052);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));

  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);

  // Reproduce the bootstrap boundary where the shared recovery SCN has not
  // advanced, while the local checkpoint is already newer than base_scn.
  // SCN::base_scn() has no physical PALF log and must map to initial LSN 0.
  const palf::LSN initial_lsn(palf::PALF_INITIAL_LSN_VAL);
  palf::LSN begin_lsn;
  ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_begin_lsn(begin_lsn));
  ASSERT_EQ(initial_lsn, begin_lsn);
  const SCN advanced_local_checkpoint_scn = SCN::scn_inc(SCN::base_scn());
  ASSERT_EQ(OB_SUCCESS,
            ls->set_clog_checkpoint(initial_lsn,
                                    advanced_local_checkpoint_scn,
                                    false /*write_slog*/));
  ASSERT_EQ(advanced_local_checkpoint_scn, ls->get_clog_checkpoint_scn());

  SCN ss_rec_scn;
  ASSERT_EQ(OB_SUCCESS,
            ls->get_ss_checkpoint_executor().get_ss_rec_scn(ss_rec_scn));
  ASSERT_TRUE(ss_rec_scn.is_min());
  SCN expected_checkpoint_scn;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaService::gen_initial_ss_checkpoint_scn_(
      ObSSLSCreateMode::BOOTSTRAP_CREATE,
      ls->get_clog_checkpoint_scn(),
      ss_rec_scn,
      expected_checkpoint_scn));
  ASSERT_EQ(SCN::base_scn(), expected_checkpoint_scn);

  ASSERT_EQ(OB_SUCCESS,
            meta_svr->create_ls(arg, ObSSLSCreateMode::BOOTSTRAP_CREATE));

  ObSSLSMeta shared_ls_meta;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(ls_id, shared_ls_meta));
  ASSERT_EQ(expected_checkpoint_scn, shared_ls_meta.get_ss_checkpoint_scn());
  ASSERT_EQ(initial_lsn, shared_ls_meta.get_ss_base_lsn());
  ASSERT_LE(shared_ls_meta.get_ss_checkpoint_scn(), ls->get_clog_checkpoint_scn());
  palf::PalfBaseInfo shared_palf_base_info;
  ASSERT_EQ(OB_SUCCESS, shared_ls_meta.get_palf_meta(shared_palf_base_info));
  ASSERT_TRUE(shared_palf_base_info.is_valid());
  ASSERT_EQ(shared_ls_meta.get_ss_base_lsn(), shared_palf_base_info.curr_lsn_);
  ASSERT_TRUE(shared_palf_base_info.prev_log_info_.scn_.is_valid());
  ASSERT_LE(shared_palf_base_info.prev_log_info_.scn_, expected_checkpoint_scn);

  // Both the LS row and its inner-tablet rows must remain idempotent.
  ASSERT_EQ(OB_SUCCESS,
            meta_svr->create_ls(arg, ObSSLSCreateMode::BOOTSTRAP_CREATE));

  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
  handle.reset();
  wait_ls_safe_destroy(ls_svr, ls_id);
  LOG_INFO("[TEST CASE END] create_ls_bootstrap_mode_uses_ss_rec_scn");
}

TEST_F(TestSharedMetaService, shared_gc_mark_deleted_ls_without_leader)
{
  using logservice::ObGarbageCollector;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObLSService *ls_svr = MTL(ObLSService *);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObGarbageCollector *gc = MTL(ObGarbageCollector *);
  ASSERT_NE(nullptr, ls_svr);
  ASSERT_NE(nullptr, meta_svr);
  ASSERT_NE(nullptr, gc);
  const ObLSID ls_id(1096);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(MTL_ID(), ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  auto cleanup = MAKE_SCOPE(ls_id, [&](const ObLSID &id) {
    ObLSHandle remaining;
    const int get_ret = ls_svr->get_ls(id, remaining, ObLSGetMod::STORAGE_MOD);
    if (OB_SUCCESS == get_ret) {
      remaining.reset();
      EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(id));
    } else {
      EXPECT_EQ(OB_LS_NOT_EXIST, get_ret);
    }
    wait_ls_safe_destroy(ls_svr, id);
  });
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));
  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);

  // One ordinary tablet is enough to reproduce the nonempty-tablet GC gate.
  ObArenaAllocator allocator("SharedGCTest", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  share::schema::ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  table_schema.set_tenant_id(MTL_ID());
  uint64_t data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), data_version));
  ObCreateTabletSchema create_schema;
  ASSERT_EQ(OB_SUCCESS, create_schema.init(allocator, table_schema,
      lib::Worker::CompatMode::MYSQL, false /*skip_column_info*/, data_version));
  const ObTabletID tablet_id(9901096);
  ObTabletHandle tablet;
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet_svr()->create_tablet(
      ls_id, tablet_id, tablet_id, SCN::base_scn(), 1 /*snapshot_version*/, create_schema,
      lib::Worker::CompatMode::MYSQL, false /*need_create_empty_major_sstable*/,
      SCN::invalid_scn(), false /*micro_index_clustered*/, false /*has_cs_replica*/,
      ObSplitTabletInfo(), ObForkTabletInfo(), data_version, tablet));
  const SCN transfer_scn = SCN::plus(SCN::base_scn(), 1);
  ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->tablet_meta_.transfer_info_.init(
      ObLSID(1001), transfer_scn, 5 /*transfer_seq*/, 5 /*private_transfer_epoch*/, SCN::min_scn()));
  {
    mds::SingleLogMdsCtx ctx{mds::MdsWriter(ObTransID(tablet_id.id()))};
    ASSERT_EQ(OB_SUCCESS, ctx.init(ls_id, ObTimeUtility::current_time() + 10_s, false /*is_replay*/));
    bool committed = false;
    auto abort_guard = MAKE_SCOPE(committed, [&](const bool &done) {
      if (!done) {
        EXPECT_EQ(OB_SUCCESS, ctx.get_mds_single_row_mvcc_ctx().trans_end(
            false, SCN::max_scn(), SCN::max_scn()));
        ctx.on_abort(SCN::max_scn());
      }
    });
    ObTabletCreateDeleteMdsUserData status(ObTabletStatus::TRANSFER_IN,
        ObTabletMdsUserDataType::START_TRANSFER_IN, transfer_scn.get_val_for_tx());
    status.transfer_scn_ = transfer_scn;
    ASSERT_EQ(OB_SUCCESS, ls->get_tablet_svr()->set_tablet_status(tablet_id, status, ctx));
    ctx.single_log_commit(transfer_scn, transfer_scn);
    committed = true;
  }
  ASSERT_TRUE(tablet.get_obj()->get_tablet_meta().has_transfer_table());

  ObGarbageCollector::ObGCCandidateArray candidates;
  ASSERT_EQ(OB_SUCCESS, gc->inner_check_ss_ls_mark_deleted_(*ls, candidates));
  ASSERT_EQ(0, candidates.count());

  const SCN offline_scn = SCN::plus(SCN::base_scn(), 1);
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::LS_OFFLINE, offline_scn));
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::WAIT_GC, SCN::invalid_scn()));
  ObSSLSMeta shared_meta;
  SCN meta_scn;
  bool marked_deleted = false;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(ls_id, shared_meta, meta_scn, marked_deleted));
  ASSERT_TRUE(marked_deleted);

  ASSERT_EQ(OB_SUCCESS, ls->set_migration_status(
      OB_MIGRATION_STATUS_MIGRATE, ls->get_rebuild_seq(), false /*write_slog*/));
  ASSERT_EQ(OB_SUCCESS, gc->inner_check_ss_ls_mark_deleted_(*ls, candidates));
  ASSERT_EQ(0, candidates.count());
  ASSERT_EQ(OB_SUCCESS, ls->set_migration_status(
      OB_MIGRATION_STATUS_MIGRATE_FAIL, ls->get_rebuild_seq(), false /*write_slog*/));

  // A committed delete must not collect an incompletely created local LS.
  const ObLSPersistentState saved_state = ls->ls_meta_.get_persistent_state();
  {
    auto restore_state = MAKE_SCOPE(saved_state, [&](const ObLSPersistentState &state) {
      ObLSMeta::ObReentrantWLockGuard guard(ls->ls_meta_.rw_lock_);
      ls->ls_meta_.ls_persistent_state_ = state;
    });
    {
      ObLSMeta::ObReentrantWLockGuard guard(ls->ls_meta_.rw_lock_);
      ls->ls_meta_.ls_persistent_state_ = ObLSPersistentState::State::LS_INIT;
    }
    ASSERT_EQ(OB_SUCCESS, gc->inner_check_ss_ls_mark_deleted_(*ls, candidates));
    ASSERT_EQ(0, candidates.count());
  }

  // The final replica may still be responsible for deleting __all_ls_status.
  // A shared tombstone alone must not remove that replica before this step.
  {
    const uint64_t exec_tenant_id = ObLSLifeIAgent::get_exec_tenant_id(MTL_ID());
    ObSqlString insert_sql;
    ObSqlString delete_sql;
    ASSERT_EQ(OB_SUCCESS, insert_sql.assign_fmt(
        "insert into oceanbase.__all_ls_status "
        "(tenant_id, ls_id, status, ls_group_id, unit_group_id, primary_zone) "
        "values (%lu, %ld, 'WAIT_OFFLINE', 0, 0, 'z1')", MTL_ID(), ls_id.id()));
    ASSERT_EQ(OB_SUCCESS, delete_sql.assign_fmt(
        "delete from oceanbase.__all_ls_status where tenant_id=%lu and ls_id=%ld",
        MTL_ID(), ls_id.id()));
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, GCTX.sql_proxy_->write(exec_tenant_id, insert_sql.ptr(), affected_rows));
    auto remove_status = MAKE_SCOPE(delete_sql, [&](const ObSqlString &sql) {
      int64_t rows = 0;
      EXPECT_EQ(OB_SUCCESS, GCTX.sql_proxy_->write(exec_tenant_id, sql.ptr(), rows));
    });
    ASSERT_EQ(OB_SUCCESS, gc->inner_check_ss_ls_mark_deleted_(*ls, candidates));
    ASSERT_EQ(0, candidates.count());
  }

  bool wait_tablet_gc = false;
  ASSERT_EQ(OB_SUCCESS, gc->check_tablet_gc_(*ls, wait_tablet_gc));
  ASSERT_TRUE(wait_tablet_gc);
  ObAddr leader;
  ASSERT_NE(OB_SUCCESS, GCTX.location_service_->nonblock_get_leader(
      obrpc::ObRpcNetHandler::CLUSTER_ID, MTL_ID(), ls_id, leader));
  // Earlier GC passes leave this LS behind; the independent shared-delete pass
  // must collect it after LS status GC without relying on a member RPC error.
  ASSERT_EQ(OB_SUCCESS, gc->gc_check_ls_migration_failed_(candidates));
  ASSERT_EQ(0, candidates.count());
  ASSERT_EQ(OB_SUCCESS, gc->gc_check_member_list_(candidates));
  ASSERT_EQ(0, candidates.count());
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, gc->gc_check_ls_status_(*ls, candidates));
  ASSERT_EQ(0, candidates.count());
  ASSERT_EQ(OB_SUCCESS, gc->gc_check_ls_mark_deleted_(candidates));
  ASSERT_EQ(1, candidates.count());
  EXPECT_EQ(ls_id, candidates.at(0).ls_id_);
  EXPECT_EQ(ObGarbageCollector::SHARED_LS_MARK_DELETED, candidates.at(0).gc_reason_);
  EXPECT_EQ(ObGarbageCollector::LS_NEED_GC, candidates.at(0).ls_status_);
  EXPECT_TRUE(tablet.get_obj()->get_tablet_meta().has_transfer_table());
  tablet.reset();
  handle.reset();
  gc->execute_gc_(candidates);
  EXPECT_EQ(OB_LS_NOT_EXIST, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  // Local removal must leave the shared tombstone readable for shared-storage GC.
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(ls_id, shared_meta, meta_scn, marked_deleted));
  EXPECT_TRUE(marked_deleted);
}

TEST_F(TestSharedMetaService, shared_gc_requires_committed_shared_delete)
{
  using logservice::ObGarbageCollector;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObLSService *ls_svr = MTL(ObLSService *);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObGarbageCollector *gc = MTL(ObGarbageCollector *);
  ASSERT_NE(nullptr, ls_svr);
  ASSERT_NE(nullptr, meta_svr);
  ASSERT_NE(nullptr, gc);
  const ObLSID ls_id(1097);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(MTL_ID(), ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  auto cleanup = MAKE_SCOPE(ls_id, [&](const ObLSID &id) {
    EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(id));
    wait_ls_safe_destroy(ls_svr, id);
  });
  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ASSERT_EQ(OB_SUCCESS, ls->set_migration_status(
      OB_MIGRATION_STATUS_MIGRATE, ls->get_rebuild_seq(), false /*write_slog*/));
  ASSERT_EQ(OB_SUCCESS, ls->set_migration_status(
      OB_MIGRATION_STATUS_MIGRATE_FAIL, ls->get_rebuild_seq(), false /*write_slog*/));
  ObGarbageCollector::ObGCCandidateArray candidates;
  // No shared lock row exists yet. A failed shared-state read must not allow GC.
  ASSERT_NE(OB_SUCCESS, gc->inner_check_ss_ls_mark_deleted_(*ls, candidates));
  ASSERT_EQ(0, candidates.count());
  ASSERT_EQ(OB_SUCCESS, gc->gc_check_ls_mark_deleted_(candidates));
  ASSERT_EQ(0, candidates.count());
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));
  ASSERT_EQ(OB_SUCCESS, gc->inner_check_ss_ls_mark_deleted_(*ls, candidates));
  ASSERT_EQ(0, candidates.count());

  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::LS_OFFLINE, SCN::plus(SCN::base_scn(), 1)));
  SetWaitGCBodyFunction set_wait_gc_body;
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_meta(ls_id,
      ObMetaUpdateReason::UPDATE_LS_GC_STATE, set_wait_gc_body, false /*check_lease*/));
  ObSSLSMeta shared_meta;
  SCN meta_scn;
  bool marked_deleted = false;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(ls_id, shared_meta, meta_scn, marked_deleted));
  logservice::LSGCState gc_state = logservice::INVALID_LS_GC_STATE;
  ASSERT_EQ(OB_SUCCESS, shared_meta.get_gc_state(gc_state));
  ASSERT_EQ(logservice::WAIT_GC, gc_state);
  ASSERT_FALSE(marked_deleted);
  ASSERT_EQ(OB_SUCCESS, gc->inner_check_ss_ls_mark_deleted_(*ls, candidates));
  ASSERT_EQ(0, candidates.count());
}

TEST_F(TestSharedMetaService, shared_gc_waits_for_transfer_dependency)
{
  using logservice::ObGarbageCollector;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObLSService *ls_svr = MTL(ObLSService *);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObGarbageCollector *gc = MTL(ObGarbageCollector *);
  ASSERT_NE(nullptr, ls_svr);
  ASSERT_NE(nullptr, meta_svr);
  ASSERT_NE(nullptr, gc);
  const ObLSID ls_ids[] = {ObLSID(1098), ObLSID(1099)};
  auto cleanup = MAKE_SCOPE(ls_svr, [&](ObLSService *service) {
    for (int64_t i = 0; i < 2; ++i) {
      ObLSHandle remaining;
      const int get_ret = service->get_ls(ls_ids[i], remaining, ObLSGetMod::STORAGE_MOD);
      if (OB_SUCCESS == get_ret) {
        remaining.reset();
        EXPECT_EQ(OB_SUCCESS, service->remove_ls(ls_ids[i]));
      } else {
        EXPECT_EQ(OB_LS_NOT_EXIST, get_ret);
      }
      wait_ls_safe_destroy(service, ls_ids[i]);
    }
  });
  ObLSHandle handles[2];
  for (int64_t i = 0; i < 2; ++i) {
    ObCreateLSArg arg;
    ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(MTL_ID(), ls_ids[i], arg));
    ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
    if (0 == i) {
      ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));
    }
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_ids[i], handles[i], ObLSGetMod::STORAGE_MOD));
    ASSERT_TRUE(handles[i].get_ls()->is_create_committed());
    ObMigrationStatus migration_status;
    ASSERT_EQ(OB_SUCCESS, handles[i].get_ls()->get_migration_status(migration_status));
    ASSERT_EQ(OB_MIGRATION_STATUS_NONE, migration_status);
  }
  ObLS *src_ls = handles[0].get_ls();
  ObLS *dest_ls = handles[1].get_ls();

  ObArenaAllocator allocator("SharedGCTest", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  share::schema::ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  table_schema.set_tenant_id(MTL_ID());
  uint64_t data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), data_version));
  ObCreateTabletSchema create_schema;
  ASSERT_EQ(OB_SUCCESS, create_schema.init(allocator, table_schema,
      lib::Worker::CompatMode::MYSQL, false /*skip_column_info*/, data_version));
  const ObTabletID tablet_id(9901098);
  const SCN transfer_scn = SCN::plus(SCN::base_scn(), 1);
  ObTabletHandle tablets[2];
  // Keep both dependency views consistent: the source tablet points to the
  // destination, whose LS meta and unfinished transfer tablet point back.
  for (int64_t i = 0; i < 2; ++i) {
    ObLSTabletService *tablet_svr = handles[i].get_ls()->get_tablet_svr();
    ASSERT_EQ(OB_SUCCESS, tablet_svr->create_tablet(
        ls_ids[i], tablet_id, tablet_id, SCN::base_scn(), 1 /*snapshot_version*/, create_schema,
        lib::Worker::CompatMode::MYSQL, false /*need_create_empty_major_sstable*/,
        SCN::invalid_scn(), false /*micro_index_clustered*/, false /*has_cs_replica*/,
        ObSplitTabletInfo(), ObForkTabletInfo(), data_version, tablets[i]));
    mds::SingleLogMdsCtx ctx{mds::MdsWriter(ObTransID(tablet_id.id() + i))};
    ASSERT_EQ(OB_SUCCESS, ctx.init(ls_ids[i], ObTimeUtility::current_time() + 10_s, false /*is_replay*/));
    bool committed = false;
    auto abort_guard = MAKE_SCOPE(committed, [&](const bool &done) {
      if (!done) {
        EXPECT_EQ(OB_SUCCESS, ctx.get_mds_single_row_mvcc_ctx().trans_end(
            false, SCN::max_scn(), SCN::max_scn()));
        ctx.on_abort(SCN::max_scn());
      }
    });
    ObTabletCreateDeleteMdsUserData status(
        0 == i ? ObTabletStatus::TRANSFER_OUT : ObTabletStatus::TRANSFER_IN,
        0 == i ? ObTabletMdsUserDataType::START_TRANSFER_OUT : ObTabletMdsUserDataType::START_TRANSFER_IN,
        transfer_scn.get_val_for_tx());
    status.transfer_ls_id_ = ls_ids[1 - i];
    status.transfer_scn_ = transfer_scn;
    ASSERT_EQ(OB_SUCCESS, tablet_svr->set_tablet_status(tablet_id, status, ctx));
    ctx.single_log_commit(transfer_scn, transfer_scn);
    committed = true;
  }
  ASSERT_EQ(OB_SUCCESS, tablets[1].get_obj()->tablet_meta_.transfer_info_.init(
      ls_ids[0], transfer_scn, 5 /*transfer_seq*/, 5 /*private_transfer_epoch*/, SCN::min_scn()));
  ObArray<ObTabletID> tablet_ids;
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id));
  ASSERT_EQ(OB_SUCCESS, dest_ls->set_transfer_meta_info(
      transfer_scn, ls_ids[0], transfer_scn, ObTransferInTransStatus::PREPARE, tablet_ids, data_version));
  ASSERT_TRUE(tablets[1].get_obj()->get_tablet_meta().has_transfer_table());

  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_ids[0], logservice::LSGCState::LS_OFFLINE, transfer_scn));
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_ids[0], logservice::LSGCState::WAIT_GC, SCN::invalid_scn()));
  ObSSLSMeta shared_meta;
  SCN meta_scn;
  bool marked_deleted = false;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(ls_ids[0], shared_meta, meta_scn, marked_deleted));
  ASSERT_TRUE(marked_deleted);
  share::ObLSStatus ls_status = share::OB_LS_NORMAL;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, gc->get_ls_status_from_table(ls_ids[0], ls_status));

  // Shared deletion must not bypass a nonfailed source LS's local dependency.
  ObGarbageCollector::ObGCCandidateArray candidates;
  ASSERT_EQ(OB_SUCCESS, gc->inner_check_ss_ls_mark_deleted_(*src_ls, candidates));
  ASSERT_EQ(0, candidates.count());
  ObMigrationStatus migration_status;
  ASSERT_EQ(OB_SUCCESS, src_ls->get_migration_status(migration_status));
  ASSERT_EQ(OB_MIGRATION_STATUS_GC, migration_status);
  ObLSHandle remaining;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_ids[0], remaining, ObLSGetMod::STORAGE_MOD));
  remaining.reset();

  // Backfill completion releases the dependency without removing the destination.
  ASSERT_EQ(OB_SUCCESS, tablets[1].get_obj()->tablet_meta_.reset_transfer_table());
  ASSERT_FALSE(tablets[1].get_obj()->get_tablet_meta().has_transfer_table());
  ASSERT_EQ(OB_SUCCESS, gc->inner_check_ss_ls_mark_deleted_(*src_ls, candidates));
  ASSERT_EQ(1, candidates.count());
  EXPECT_EQ(ls_ids[0], candidates.at(0).ls_id_);
  EXPECT_EQ(ObGarbageCollector::SHARED_LS_MARK_DELETED, candidates.at(0).gc_reason_);
  EXPECT_EQ(ObGarbageCollector::LS_NEED_GC, candidates.at(0).ls_status_);
  tablets[0].reset();
  handles[0].reset();
  gc->execute_gc_(candidates);
  EXPECT_EQ(OB_LS_NOT_EXIST, ls_svr->get_ls(ls_ids[0], remaining, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_ids[1], remaining, ObLSGetMod::STORAGE_MOD));
  ObTabletHandle dest_tablet;
  ASSERT_EQ(OB_SUCCESS, remaining.get_ls()->ha_get_tablet(tablet_id, dest_tablet));
  EXPECT_FALSE(dest_tablet.get_obj()->get_tablet_meta().has_transfer_table());
}

TEST_F(TestSharedMetaService, gc_state_update_is_monotonic_and_idempotent)
{
  LOG_INFO("[TEST CASE BEGIN] gc_state_update_is_monotonic_and_idempotent");
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  const ObLSID ls_id(1070);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));

  share::SCN offline_scn1;
  share::SCN offline_scn2;
  share::SCN offline_scn3;
  ASSERT_EQ(OB_SUCCESS, offline_scn1.convert_from_ts(1));
  ASSERT_EQ(OB_SUCCESS, offline_scn2.convert_from_ts(2));
  ASSERT_EQ(OB_SUCCESS, offline_scn3.convert_from_ts(3));
  ASSERT_LT(offline_scn1, offline_scn2);
  ASSERT_LT(offline_scn2, offline_scn3);

  // These sequential calls model the order after SSWriter serialization.
  // Repeating the same OFFLINE request must not create another durable version.
  {
    ObLSHandle handle;
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ObLS *ls = handle.get_ls();
    ASSERT_NE(nullptr, ls);
    logservice::ObGCHandler *gc_handler = ls->get_gc_handler();
    ASSERT_NE(nullptr, gc_handler);
    ASSERT_EQ(OB_SUCCESS, gc_handler->update_ss_ls_meta_(
        ls_id, logservice::LSGCState::LS_OFFLINE, offline_scn1));
  }
  // The remaining checks only need shared metadata. Removing the local LS also
  // prevents the background checkpoint task from changing its LS meta_scn.
  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
  wait_ls_safe_destroy(ls_svr, ls_id);

  ObSSLSMeta first_offline_meta;
  share::SCN first_offline_meta_scn;
  bool first_offline_mark_deleted = false;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(
      ls_id, first_offline_meta, first_offline_meta_scn, first_offline_mark_deleted));
  ASSERT_FALSE(first_offline_mark_deleted);

  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::LS_OFFLINE, offline_scn1));
  ObSSLSMeta repeated_offline_meta;
  share::SCN repeated_offline_meta_scn;
  bool repeated_offline_mark_deleted = false;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(
      ls_id, repeated_offline_meta, repeated_offline_meta_scn, repeated_offline_mark_deleted));
  EXPECT_EQ(first_offline_meta_scn, repeated_offline_meta_scn);
  ASSERT_FALSE(repeated_offline_mark_deleted);

  // A newer OFFLINE advances offline_scn. A stale request arriving afterwards
  // succeeds as a no-op and does not create another durable version.
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::LS_OFFLINE, offline_scn2));
  ObSSLSMeta newer_offline_meta;
  share::SCN newer_offline_meta_scn;
  bool newer_offline_mark_deleted = false;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(
      ls_id, newer_offline_meta, newer_offline_meta_scn, newer_offline_mark_deleted));
  EXPECT_NE(first_offline_meta_scn, newer_offline_meta_scn);
  ASSERT_FALSE(newer_offline_mark_deleted);

  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::LS_OFFLINE, offline_scn1));
  ObSSLSMeta stale_offline_meta;
  share::SCN stale_offline_meta_scn;
  bool stale_offline_mark_deleted = false;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(
      ls_id, stale_offline_meta, stale_offline_meta_scn, stale_offline_mark_deleted));
  EXPECT_EQ(newer_offline_meta_scn, stale_offline_meta_scn);
  ASSERT_FALSE(stale_offline_mark_deleted);

  logservice::LSGCState offline_gc_state = logservice::INVALID_LS_GC_STATE;
  share::SCN durable_offline_scn;
  ASSERT_EQ(OB_SUCCESS, stale_offline_meta.get_gc_state(offline_gc_state));
  ASSERT_EQ(OB_SUCCESS, stale_offline_meta.get_offline_scn(durable_offline_scn));
  EXPECT_EQ(logservice::LSGCState::LS_OFFLINE, offline_gc_state);
  EXPECT_EQ(offline_scn2, durable_offline_scn);

  // Once WAIT_GC has marked the LS row deleted, repeated WAIT_GC and delayed
  // OFFLINE requests are covered by the same durable terminal state.
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::WAIT_GC, share::SCN::invalid_scn()));
  ObSSLSMeta wait_gc_meta;
  share::SCN wait_gc_meta_scn;
  bool wait_gc_mark_deleted = false;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(
      ls_id, wait_gc_meta, wait_gc_meta_scn, wait_gc_mark_deleted));
  ASSERT_TRUE(wait_gc_mark_deleted);

  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::WAIT_GC, share::SCN::invalid_scn()));
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::LS_OFFLINE, offline_scn1));
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::LS_OFFLINE, offline_scn2));
  EXPECT_EQ(OB_STATE_NOT_MATCH, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::LS_OFFLINE, offline_scn3));

  ObSSLSMeta final_ls_meta;
  share::SCN final_meta_scn;
  bool is_mark_deleted = false;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(
      ls_id, final_ls_meta, final_meta_scn, is_mark_deleted));
  logservice::LSGCState final_gc_state = logservice::INVALID_LS_GC_STATE;
  share::SCN final_offline_scn;
  ASSERT_EQ(OB_SUCCESS, final_ls_meta.get_gc_state(final_gc_state));
  ASSERT_EQ(OB_SUCCESS, final_ls_meta.get_offline_scn(final_offline_scn));
  EXPECT_EQ(wait_gc_meta_scn, final_meta_scn);
  EXPECT_EQ(logservice::LSGCState::WAIT_GC, final_gc_state);
  EXPECT_EQ(offline_scn2, final_offline_scn);
  EXPECT_TRUE(is_mark_deleted);

  LOG_INFO("[TEST CASE END] gc_state_update_is_monotonic_and_idempotent");
}

TEST_F(TestSharedMetaService, replay_offline_falls_back_when_sswriter_addr_is_invalid)
{
  LOG_INFO("[TEST CASE BEGIN] replay_offline_falls_back_when_sswriter_addr_is_invalid");
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  share::SCN offline_scn1;
  share::SCN offline_scn2;
  ASSERT_EQ(OB_SUCCESS, offline_scn1.convert_from_ts(1));
  ASSERT_EQ(OB_SUCCESS, offline_scn2.convert_from_ts(2));
  ASSERT_LT(offline_scn1, offline_scn2);

  // Model the incident: the old leader persisted shared LS meta, while the
  // new leader still has to replay the committed OFFLINE_LS log locally.
  const ObLSID covered_ls_id(1072);
  ObCreateLSArg covered_ls_arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, covered_ls_id, covered_ls_arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(covered_ls_arg));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(covered_ls_arg, ObSSLSCreateMode::NORMAL_CREATE));
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      covered_ls_id, logservice::LSGCState::LS_OFFLINE, offline_scn1));
  ObSSLSMeta covered_meta_before;
  share::SCN covered_meta_scn_before;
  bool covered_mark_deleted_before = false;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(
      covered_ls_id, covered_meta_before, covered_meta_scn_before, covered_mark_deleted_before));
  ASSERT_FALSE(covered_mark_deleted_before);

  mock_is_sswriter = false;
  mock_sswriter_addr_valid = false;

  ObLSHandle covered_ls_handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(
      covered_ls_id, covered_ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS *covered_ls = covered_ls_handle.get_ls();
  ASSERT_NE(nullptr, covered_ls);
  logservice::ObGCHandler *covered_gc_handler = covered_ls->get_gc_handler();
  ASSERT_NE(nullptr, covered_gc_handler);
  logservice::LSGCState covered_gc_state_before = logservice::INVALID_LS_GC_STATE;
  ASSERT_EQ(OB_SUCCESS, covered_ls->get_gc_state(covered_gc_state_before));
  ASSERT_EQ(logservice::LSGCState::NORMAL, covered_gc_state_before);

  covered_gc_handler->rec_scn_.atomic_set(offline_scn1);
  ASSERT_FALSE(covered_gc_handler->get_rec_scn().is_max());
  check_lease_call_count = 0;
  ASSERT_EQ(OB_SUCCESS, replay_offline_log(covered_gc_handler, offline_scn1));
  EXPECT_GT(check_lease_call_count, 0);
  EXPECT_TRUE(covered_gc_handler->get_rec_scn().is_max());

  logservice::LSGCState covered_local_gc_state = logservice::INVALID_LS_GC_STATE;
  share::SCN covered_local_offline_scn;
  ASSERT_EQ(OB_SUCCESS, covered_ls->get_gc_state(covered_local_gc_state));
  ASSERT_EQ(OB_SUCCESS, covered_ls->get_offline_scn(covered_local_offline_scn));
  EXPECT_EQ(logservice::LSGCState::LS_OFFLINE, covered_local_gc_state);
  EXPECT_EQ(offline_scn1, covered_local_offline_scn);

  ObSSLSMeta covered_meta_after;
  share::SCN covered_meta_scn_after;
  bool covered_mark_deleted_after = false;
  logservice::LSGCState covered_shared_gc_state = logservice::INVALID_LS_GC_STATE;
  share::SCN covered_shared_offline_scn;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(
      covered_ls_id, covered_meta_after, covered_meta_scn_after, covered_mark_deleted_after));
  ASSERT_EQ(OB_SUCCESS, covered_meta_after.get_gc_state(covered_shared_gc_state));
  ASSERT_EQ(OB_SUCCESS, covered_meta_after.get_offline_scn(covered_shared_offline_scn));
  EXPECT_EQ(covered_meta_scn_before, covered_meta_scn_after);
  EXPECT_FALSE(covered_mark_deleted_after);
  EXPECT_EQ(logservice::LSGCState::LS_OFFLINE, covered_shared_gc_state);
  EXPECT_EQ(offline_scn1, covered_shared_offline_scn);

  covered_ls_handle.reset();
  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(covered_ls_id));
  wait_ls_safe_destroy(ls_svr, covered_ls_id);

  // Model the crash window: OFFLINE_LS is committed but shared LS meta has not
  // been persisted. Replay must perform the monotonic atomic update without a
  // lease, while the normal control path must continue checking the lease.
  const ObLSID missing_ls_id(1073);
  ObCreateLSArg missing_ls_arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, missing_ls_id, missing_ls_arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(missing_ls_arg));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(missing_ls_arg, ObSSLSCreateMode::NORMAL_CREATE));

  ObLSHandle missing_ls_handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(
      missing_ls_id, missing_ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS *missing_ls = missing_ls_handle.get_ls();
  ASSERT_NE(nullptr, missing_ls);
  logservice::ObGCHandler *missing_gc_handler = missing_ls->get_gc_handler();
  ASSERT_NE(nullptr, missing_gc_handler);
  logservice::LSGCState missing_gc_state_before = logservice::INVALID_LS_GC_STATE;
  ASSERT_EQ(OB_SUCCESS, missing_ls->get_gc_state(missing_gc_state_before));
  ASSERT_EQ(logservice::LSGCState::NORMAL, missing_gc_state_before);

  check_lease_call_count = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, meta_svr->update_ls_gc_state(
      missing_ls_id, logservice::LSGCState::LS_OFFLINE, offline_scn1));
  EXPECT_GT(check_lease_call_count, 0);

  ObSSLSMeta submit_meta;
  logservice::LSGCState submit_gc_state = logservice::INVALID_LS_GC_STATE;
  share::SCN submit_offline_scn;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(missing_ls_id, submit_meta));
  ASSERT_EQ(OB_SUCCESS, submit_meta.get_gc_state(submit_gc_state));
  ASSERT_EQ(OB_SUCCESS, submit_meta.get_offline_scn(submit_offline_scn));
  EXPECT_EQ(logservice::LSGCState::NORMAL, submit_gc_state);
  EXPECT_FALSE(submit_offline_scn.is_valid());

  check_lease_call_count = 0;
  ASSERT_EQ(OB_SUCCESS, replay_offline_log(missing_gc_handler, offline_scn1));
  EXPECT_GT(check_lease_call_count, 0);

  ObSSLSMeta replay_written_meta;
  share::SCN replay_written_meta_scn;
  bool replay_written_mark_deleted = false;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(
      missing_ls_id, replay_written_meta, replay_written_meta_scn, replay_written_mark_deleted));
  logservice::LSGCState replay_written_gc_state = logservice::INVALID_LS_GC_STATE;
  share::SCN replay_written_offline_scn;
  ASSERT_EQ(OB_SUCCESS, replay_written_meta.get_gc_state(replay_written_gc_state));
  ASSERT_EQ(OB_SUCCESS, replay_written_meta.get_offline_scn(replay_written_offline_scn));
  EXPECT_EQ(logservice::LSGCState::LS_OFFLINE, replay_written_gc_state);
  EXPECT_EQ(offline_scn1, replay_written_offline_scn);
  ASSERT_FALSE(replay_written_mark_deleted);

  EXPECT_EQ(OB_INVALID_ARGUMENT, meta_svr->update_ls_gc_state(
      missing_ls_id,
      logservice::LSGCState::LS_OFFLINE,
      share::SCN::invalid_scn(),
      ObSSMetaService::DEFAULT_TIMEOUT_US,
      true /*is_replay*/));
  EXPECT_EQ(OB_ERR_UNEXPECTED, meta_svr->update_ls_gc_state(
      missing_ls_id,
      logservice::LSGCState::WAIT_GC,
      share::SCN::invalid_scn(),
      ObSSMetaService::DEFAULT_TIMEOUT_US,
      true /*is_replay*/));
  EXPECT_EQ(OB_INVALID_ARGUMENT, meta_svr->update_for_ss_gc_state_local_(
      missing_ls_id,
      ObMetaUpdateReason::UPDATE_LS_GC_STATE,
      false /*check_lease*/,
      logservice::LSGCState::WAIT_GC,
      share::SCN::invalid_scn()));

  missing_ls_handle.reset();
  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(missing_ls_id));
  wait_ls_safe_destroy(ls_svr, missing_ls_id);

  mock_is_sswriter = true;
  mock_sswriter_addr_valid = true;
  check_lease_call_count = 0;
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      missing_ls_id, logservice::LSGCState::LS_OFFLINE, offline_scn2));
  EXPECT_GT(check_lease_call_count, 0);
  LOG_INFO("[TEST CASE END] replay_offline_falls_back_when_sswriter_addr_is_invalid");
}

TEST_F(TestSharedMetaService, incomplete_wait_gc_is_not_repaired)
{
  LOG_INFO("[TEST CASE BEGIN] incomplete_wait_gc_is_not_repaired");
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  const ObLSID ls_id(1071);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));
  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
  wait_ls_safe_destroy(ls_svr, ls_id);

  share::SCN offline_scn;
  ASSERT_EQ(OB_SUCCESS, offline_scn.convert_from_ts(1));
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::LS_OFFLINE, offline_scn));

  // Simulate a historical partial state: the body reached WAIT_GC through a
  // generic LS-meta update, while the LS head and lock row remain unmarked.
  SetWaitGCBodyFunction set_wait_gc_body;
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_meta(
      ls_id,
      ObMetaUpdateReason::UPDATE_LS_GC_STATE,
      set_wait_gc_body,
      false /*check_lease*/));

  ObSSLSMeta partial_ls_meta;
  share::SCN partial_meta_scn;
  bool is_mark_deleted = false;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(
      ls_id, partial_ls_meta, partial_meta_scn, is_mark_deleted));
  logservice::LSGCState partial_gc_state = logservice::INVALID_LS_GC_STATE;
  ASSERT_EQ(OB_SUCCESS, partial_ls_meta.get_gc_state(partial_gc_state));
  ASSERT_EQ(logservice::LSGCState::WAIT_GC, partial_gc_state);
  ASSERT_FALSE(is_mark_deleted);

  EXPECT_EQ(OB_STATE_NOT_MATCH, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::WAIT_GC, share::SCN::invalid_scn()));

  ObSSLSMeta final_ls_meta;
  share::SCN final_meta_scn;
  ASSERT_EQ(OB_SUCCESS, get_complete_ls_meta(
      ls_id, final_ls_meta, final_meta_scn, is_mark_deleted));
  logservice::LSGCState final_gc_state = logservice::INVALID_LS_GC_STATE;
  share::SCN final_offline_scn;
  ASSERT_EQ(OB_SUCCESS, final_ls_meta.get_gc_state(final_gc_state));
  ASSERT_EQ(OB_SUCCESS, final_ls_meta.get_offline_scn(final_offline_scn));
  EXPECT_EQ(partial_meta_scn, final_meta_scn);
  EXPECT_EQ(logservice::LSGCState::WAIT_GC, final_gc_state);
  EXPECT_EQ(offline_scn, final_offline_scn);
  EXPECT_FALSE(is_mark_deleted);

  LOG_INFO("[TEST CASE END] incomplete_wait_gc_is_not_repaired");
}

TEST_F(TestSharedMetaService, wait_gc_update_failure_keeps_ls_status)
{
  LOG_INFO("[TEST CASE BEGIN] wait_gc_update_failure_keeps_ls_status");
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  const ObLSID ls_id(1080);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));

  ObSSLSMeta shared_ls_meta;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(ls_id, shared_ls_meta));
  share::SCN shared_offline_scn;
  ASSERT_EQ(OB_SUCCESS, shared_ls_meta.get_offline_scn(shared_offline_scn));
  ASSERT_FALSE(shared_offline_scn.is_valid());

  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  logservice::ObGCHandler *gc_handler = ls->get_gc_handler();
  ASSERT_NE(nullptr, gc_handler);

  share::SCN offline_scn;
  ASSERT_EQ(OB_SUCCESS, offline_scn.convert_from_ts(1));
  ASSERT_EQ(OB_SUCCESS, ls->set_gc_state(logservice::LSGCState::LS_OFFLINE, offline_scn));
  logservice::ObGarbageCollector::LSStatus ls_status =
      logservice::ObGarbageCollector::LSStatus::LS_WAIT_OFFLINE;

  const int ret = gc_handler->try_check_and_set_wait_gc_when_log_archive_is_off_(
      logservice::LSGCState::LS_OFFLINE, offline_scn, offline_scn, ls_status);

  EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_EQ(logservice::ObGarbageCollector::LSStatus::LS_WAIT_OFFLINE, ls_status);
  logservice::LSGCState local_gc_state = logservice::INVALID_LS_GC_STATE;
  const int get_gc_state_ret = ls->get_gc_state(local_gc_state);
  ObSSLSMeta final_shared_ls_meta;
  const int get_shared_ls_meta_ret = meta_svr->get_ls_meta(ls_id, final_shared_ls_meta);
  logservice::LSGCState final_shared_gc_state = logservice::INVALID_LS_GC_STATE;
  share::SCN final_shared_offline_scn;
  const int get_shared_gc_state_ret =
      OB_SUCCESS == get_shared_ls_meta_ret
          ? final_shared_ls_meta.get_gc_state(final_shared_gc_state)
          : get_shared_ls_meta_ret;
  const int get_shared_offline_scn_ret =
      OB_SUCCESS == get_shared_ls_meta_ret
          ? final_shared_ls_meta.get_offline_scn(final_shared_offline_scn)
          : get_shared_ls_meta_ret;
  handle.reset();
  const int remove_ls_ret = ls_svr->remove_ls(ls_id);
  if (OB_SUCCESS == remove_ls_ret) {
    wait_ls_safe_destroy(ls_svr, ls_id);
  }
  EXPECT_EQ(OB_SUCCESS, get_gc_state_ret);
  EXPECT_EQ(logservice::LSGCState::LS_OFFLINE, local_gc_state);
  EXPECT_EQ(OB_SUCCESS, get_shared_ls_meta_ret);
  EXPECT_EQ(OB_SUCCESS, get_shared_gc_state_ret);
  EXPECT_EQ(OB_SUCCESS, get_shared_offline_scn_ret);
  EXPECT_EQ(logservice::LSGCState::NORMAL, final_shared_gc_state);
  EXPECT_FALSE(final_shared_offline_scn.is_valid());
  EXPECT_EQ(OB_SUCCESS, remove_ls_ret);
  LOG_INFO("[TEST CASE END] wait_gc_update_failure_keeps_ls_status");
}

TEST_F(TestSharedMetaService, local_wait_gc_failure_keeps_ls_status)
{
  LOG_INFO("[TEST CASE BEGIN] local_wait_gc_failure_keeps_ls_status");
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  const ObLSID ls_id(1081);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));

  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  logservice::ObGCHandler *gc_handler = ls->get_gc_handler();
  ASSERT_NE(nullptr, gc_handler);

  share::SCN offline_scn;
  ASSERT_EQ(OB_SUCCESS, offline_scn.convert_from_ts(1));
  ASSERT_EQ(OB_SUCCESS, ls->set_gc_state(logservice::LSGCState::LS_OFFLINE, offline_scn));
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(
      ls_id, logservice::LSGCState::LS_OFFLINE, offline_scn));

  // ZOMBIE LS meta rejects subsequent local updates, while shared meta remains
  // writable. This models the second step failing after durable WAIT_GC succeeds.
  logservice::ObGarbageCollector::LSStatus ls_status =
      logservice::ObGarbageCollector::LSStatus::LS_WAIT_OFFLINE;
  const int set_remove_state_ret = ls->set_remove_state(false /*write_slog*/);
  int update_ret = OB_SUCCESS;
  if (OB_SUCCESS == set_remove_state_ret) {
    update_ret = gc_handler->try_check_and_set_wait_gc_when_log_archive_is_off_(
        logservice::LSGCState::LS_OFFLINE, offline_scn, offline_scn, ls_status);
  }

  logservice::LSGCState local_gc_state = logservice::INVALID_LS_GC_STATE;
  const int get_local_gc_state_ret = ls->get_gc_state(local_gc_state);

  ObSSLSMeta final_shared_ls_meta;
  share::SCN final_meta_scn;
  bool is_mark_deleted = false;
  const int get_shared_ls_meta_ret = get_complete_ls_meta(
      ls_id, final_shared_ls_meta, final_meta_scn, is_mark_deleted);
  logservice::LSGCState final_shared_gc_state = logservice::INVALID_LS_GC_STATE;
  share::SCN final_shared_offline_scn;
  const int get_shared_gc_state_ret =
      OB_SUCCESS == get_shared_ls_meta_ret
          ? final_shared_ls_meta.get_gc_state(final_shared_gc_state)
          : get_shared_ls_meta_ret;
  const int get_shared_offline_scn_ret =
      OB_SUCCESS == get_shared_ls_meta_ret
          ? final_shared_ls_meta.get_offline_scn(final_shared_offline_scn)
          : get_shared_ls_meta_ret;
  handle.reset();
  const int remove_ls_ret = ls_svr->remove_ls(ls_id);
  if (OB_SUCCESS == remove_ls_ret) {
    wait_ls_safe_destroy(ls_svr, ls_id);
  }

  EXPECT_EQ(OB_SUCCESS, set_remove_state_ret);
  EXPECT_EQ(OB_STATE_NOT_MATCH, update_ret);
  EXPECT_EQ(logservice::ObGarbageCollector::LSStatus::LS_WAIT_OFFLINE, ls_status);
  EXPECT_EQ(OB_SUCCESS, get_local_gc_state_ret);
  EXPECT_EQ(logservice::LSGCState::LS_OFFLINE, local_gc_state);
  EXPECT_EQ(OB_SUCCESS, get_shared_ls_meta_ret);
  EXPECT_EQ(OB_SUCCESS, get_shared_gc_state_ret);
  EXPECT_EQ(OB_SUCCESS, get_shared_offline_scn_ret);
  EXPECT_EQ(logservice::LSGCState::WAIT_GC, final_shared_gc_state);
  EXPECT_EQ(offline_scn, final_shared_offline_scn);
  EXPECT_TRUE(is_mark_deleted);
  EXPECT_EQ(OB_SUCCESS, remove_ls_ret);
  LOG_INFO("[TEST CASE END] local_wait_gc_failure_keeps_ls_status");
}

// ====================== Category 1: LS-related interfaces ======================

TEST_F(TestSharedMetaService, get_ls_meta_with_read_param)
{
  LOG_INFO("[TEST CASE BEGIN] get_ls_meta_with_read_param");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  const ObLSID ls_id(1010);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));

  ObSSMetaReadParamV2 read_param;
  read_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::LS_META,
      ls_id,
      share::SCN::max_scn(),
      true /*is_in_table*/);
  read_param.set_read_mark_delete(true);

  ObSSLSMeta ls_meta;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(read_param, share::SCN::max_scn(), ls_meta));
  ASSERT_TRUE(ls_meta.is_valid());
  LOG_INFO("get_ls_meta_with_read_param succeed", K(ls_id), K(ls_meta));

  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
  LOG_INFO("[TEST CASE END] get_ls_meta_with_read_param");
}

TEST_F(TestSharedMetaService, get_ls_id_iter_basic)
{
  LOG_INFO("[TEST CASE BEGIN] get_ls_id_iter_basic");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  const ObLSID ls_id(1020);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));

  ObSSMetaIterGuard<ObSSMetaLSIDIter> iter_guard;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_id_iter(share::SCN::max_scn(), iter_guard));

  ObSSMetaLSIDIter *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
  ASSERT_NE(nullptr, iter);

  ObLSID out_ls_id;
  bool found_sslog_ls = false;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = iter->get_next(out_ls_id))) {
    LOG_INFO("get_ls_id_iter: got ls_id", K(out_ls_id));
    if (out_ls_id == ls_id) {
      found_sslog_ls = true;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_TRUE(found_sslog_ls);

  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
  LOG_INFO("[TEST CASE END] get_ls_id_iter_basic");
}

TEST_F(TestSharedMetaService, get_ls_scn_iter_basic)
{
  LOG_INFO("[TEST CASE BEGIN] get_ls_scn_iter_basic");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  const ObLSID ls_id(1030);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));

  ObSSMetaReadParamV2 read_param;
  read_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::LS_META,
      ls_id,
      share::SCN::min_scn(),
      true /*is_in_table*/);

  ObSSMetaIterGuard<ObSSMetaLSSCNIterator> iter_guard;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_scn_iter(read_param, share::SCN::max_scn(), iter_guard));

  ObSSMetaLSSCNIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
  ASSERT_NE(nullptr, iter);

  share::SCN scn;
  int64_t count = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = iter->get_next(scn))) {
    ASSERT_TRUE(scn.is_valid());
    LOG_INFO("get_ls_scn_iter: got scn", K(scn));
    count++;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GE(count, 1);

  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
  LOG_INFO("[TEST CASE END] get_ls_scn_iter_basic");
}

TEST_F(TestSharedMetaService, remove_ls_basic)
{
  LOG_INFO("[TEST CASE BEGIN] remove_ls_basic");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  const ObLSID ls_id(1040);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));

  // get ls_meta scn via get_ls_scn_iter
  ObSSMetaReadParamV2 read_param;
  read_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::LS_META,
      ls_id,
      share::SCN::min_scn(),
      true /*is_in_table*/);

  share::SCN meta_scn;
  {
    ObSSMetaIterGuard<ObSSMetaLSSCNIterator> iter_guard;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_scn_iter(read_param, share::SCN::max_scn(), iter_guard));
    ObSSMetaLSSCNIterator *iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
    ASSERT_EQ(OB_SUCCESS, iter->get_next(meta_scn));
    ASSERT_TRUE(meta_scn.is_valid());
  }

  // remove the ls meta row
  ASSERT_EQ(OB_SUCCESS, meta_svr->remove_ls(ls_id, meta_scn));

  // verify ls_meta is gone
  ObSSLSMeta ls_meta;
  int ret = meta_svr->get_ls_meta(ls_id, ls_meta);
  ASSERT_EQ(OB_LS_NOT_EXIST, ret);
  LOG_INFO("remove_ls_basic: ls removed successfully", K(ls_id), K(meta_scn));

  // re-create for subsequent tests
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(ls_id, ls_meta));
  ASSERT_TRUE(ls_meta.is_valid());

  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
  LOG_INFO("[TEST CASE END] remove_ls_basic");
}

// ====================== Category 2: Raw meta / tablet_meta row-level interfaces ======================

TEST_F(TestSharedMetaService, insert_and_read_tablet_meta)
{
  LOG_INFO("[TEST CASE BEGIN] insert_and_read_tablet_meta");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  // Use a synthetic ls_id (8001) outside the real user-LS range of tt1, so
  // this row is never picked up by tablet-iter cases (e.g.
  // get_tablet_iter_basic) that scan run_ctx_.ls_id_ and require a fully
  // serialized ObSSTabletMetaValue.
  ObSSMetaTabletKey key;
  key.ls_id_ = ObLSID(8001);
  key.tablet_id_ = ObTabletID(9900001);
  key.reorganization_scn_ = share::SCN::min_scn();
  key.meta_scn_ = share::SCN::base_scn();
  ASSERT_TRUE(key.is_valid());

  // meta_value is a serialized ObSSMetaHeadValue so the row is at least
  // head-deserializable for any iterator's check_mark_deleted path.
  ObArenaAllocator allocator("TestReadMeta");
  ObString value_str;
  ASSERT_EQ(OB_SUCCESS, build_head_value_str(allocator,
                                              ObMetaUpdateReason::CREATE_TABLET,
                                              value_str));
  ObSSMetaTabletRow row(static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META), key, value_str);

  ASSERT_EQ(OB_SUCCESS, meta_svr->insert_tablet_meta(row));
  LOG_INFO("insert_tablet_meta succeed", K(key));

  // read back
  ObSSMetaTabletRow out_row;
  ASSERT_EQ(OB_SUCCESS, meta_svr->read_tablet_meta(key, allocator, out_row));
  ASSERT_EQ(out_row.key_, key);
  ASSERT_EQ(value_str.length(), out_row.value_.length());
  ASSERT_EQ(0, MEMCMP(out_row.value_.ptr(), value_str.ptr(), value_str.length()));
  LOG_INFO("read_tablet_meta succeed", K(out_row));

  // cleanup
  ASSERT_EQ(OB_SUCCESS, delete_tablet_meta_row(*meta_svr, key));
  LOG_INFO("[TEST CASE END] insert_and_read_tablet_meta");
}

TEST_F(TestSharedMetaService, batch_insert_and_read_tablet_metas)
{
  LOG_INFO("[TEST CASE BEGIN] batch_insert_and_read_tablet_metas");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  // Synthetic ls_id (8002) to isolate from run_ctx_.ls_id_ (see comment in
  // insert_and_read_tablet_meta).
  ObArenaAllocator allocator("TestBatchRead");
  const int64_t batch_count = 3;
  ObSEArray<ObSSMetaTabletRow, SSMETA_ROW_BATCH_CNT> insert_rows;
  ObSEArray<ObSSMetaTabletKey, SSMETA_ROW_BATCH_CNT> keys;
  ObSEArray<ObString, SSMETA_ROW_BATCH_CNT> value_strs;

  for (int64_t i = 0; i < batch_count; i++) {
    ObSSMetaTabletKey key;
    key.ls_id_ = ObLSID(8002);
    key.tablet_id_ = ObTabletID(9900101 + i);
    key.reorganization_scn_ = share::SCN::min_scn();
    key.meta_scn_ = share::SCN::base_scn();
    ASSERT_TRUE(key.is_valid());

    ObString val;
    ASSERT_EQ(OB_SUCCESS, build_head_value_str(allocator,
                                                ObMetaUpdateReason::CREATE_TABLET,
                                                val));
    ObSSMetaTabletRow row(static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META), key, val);
    ASSERT_EQ(OB_SUCCESS, insert_rows.push_back(row));
    ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
    ASSERT_EQ(OB_SUCCESS, value_strs.push_back(val));
  }

  ASSERT_EQ(OB_SUCCESS, meta_svr->batch_insert_tablet_metas(insert_rows));
  LOG_INFO("batch_insert_tablet_metas succeed", K(batch_count));

  ObSEArray<ObSSMetaTabletRow, SSMETA_ROW_BATCH_CNT> out_rows;
  ASSERT_EQ(OB_SUCCESS, meta_svr->batch_read_tablet_metas(keys, allocator, out_rows));
  ASSERT_EQ(batch_count, out_rows.count());

  for (int64_t i = 0; i < batch_count; i++) {
    ASSERT_EQ(out_rows.at(i).key_, keys.at(i));
    ASSERT_EQ(value_strs.at(i).length(), out_rows.at(i).value_.length());
    ASSERT_EQ(0, MEMCMP(out_rows.at(i).value_.ptr(), value_strs.at(i).ptr(),
                        value_strs.at(i).length()));
  }
  LOG_INFO("batch_read_tablet_metas succeed", K(out_rows.count()));

  // cleanup
  for (int64_t i = 0; i < batch_count; i++) {
    ASSERT_EQ(OB_SUCCESS, delete_tablet_meta_row(*meta_svr, keys.at(i)));
  }
  LOG_INFO("[TEST CASE END] batch_insert_and_read_tablet_metas");
}

TEST_F(TestSharedMetaService, insert_and_get_raw_meta)
{
  LOG_INFO("[TEST CASE BEGIN] insert_and_get_raw_meta");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  // Synthetic ls_id (8003) to isolate from run_ctx_.ls_id_ (see comment in
  // insert_and_read_tablet_meta). meta_key is built the same way as the
  // production write path (ObSSMetaTabletKey -> macro_id -> path string).
  ObSSMetaTabletKey tablet_key;
  tablet_key.ls_id_ = ObLSID(8003);
  tablet_key.tablet_id_ = ObTabletID(9900401);
  tablet_key.reorganization_scn_ = share::SCN::min_scn();
  tablet_key.meta_scn_ = share::SCN::base_scn();
  ASSERT_TRUE(tablet_key.is_valid());

  blocksstable::MacroBlockId macro_id = tablet_key.convert_to_macro_id(true /*is_in_table*/);
  char path_buf[512] = {0};
  int64_t path_pos = 0;
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::to_relative_remote_path_format(
      macro_id, path_buf, sizeof(path_buf), path_pos));
  ObString meta_key_str(static_cast<int32_t>(path_pos), path_buf);

  // meta_value: serialized ObSSMetaHeadValue (head-deserializable).
  ObArenaAllocator allocator("TestRawMeta");
  ObString value_str;
  ASSERT_EQ(OB_SUCCESS, build_head_value_str(allocator,
                                              ObMetaUpdateReason::CREATE_TABLET,
                                              value_str));

  // sanity: head section is well-formed (the same check downstream iter paths run).
  bool is_deleted = true;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaHeadValue::check_mark_deleted(value_str, is_deleted));
  ASSERT_FALSE(is_deleted);

  ObSSRawMetaRow row(tenant_id,
                     tablet_key.ls_id_,
                     static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META),
                     meta_key_str,
                     value_str);

  ASSERT_EQ(OB_SUCCESS, meta_svr->insert_raw_meta(row));
  LOG_INFO("insert_raw_meta succeed", K(row));

  // read back via get_raw_meta using the matching tablet-level read param.
  ObSSMetaReadParamV2 read_param;
  read_param.set_tablet_level_param(
      ObSSMetaReadParamType::TABLET_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      tablet_key.ls_id_, tablet_key.tablet_id_,
      share::SCN::min_scn(), tablet_key.reorganization_scn_,
      true /*is_in_table*/);
  read_param.set_read_mark_delete(true);

  ObSSRawMetaRow out_row;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_raw_meta(read_param, share::SCN::max_scn(),
                                                allocator, out_row));
  ASSERT_EQ(tenant_id, out_row.tenant_id_);
  ASSERT_EQ(tablet_key.ls_id_, out_row.ls_id_);
  ASSERT_EQ(static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META), out_row.meta_type_);
  ASSERT_EQ(meta_key_str.length(), out_row.meta_key_.length());
  ASSERT_EQ(0, MEMCMP(out_row.meta_key_.ptr(), meta_key_str.ptr(), meta_key_str.length()));

  // verify the read-back meta_value can be deserialized as ObSSMetaHeadValue
  ObSSMetaHeadValue out_head_value;
  int64_t out_pos = 0;
  ASSERT_EQ(OB_SUCCESS, out_head_value.deserialize_value_info(out_row.meta_value_.ptr(),
                                                              out_row.meta_value_.length(),
                                                              out_pos));
  ASSERT_TRUE(out_head_value.get_meta_head_info().is_valid());
  ASSERT_FALSE(out_head_value.get_meta_head_info().is_mark_deleted());
  ObMetaUpdateReason out_reason = ObMetaUpdateReason::INVALID_META_UPDATE_REASON;
  out_head_value.get_meta_head_info().get_update_reason(out_reason);
  ASSERT_EQ(ObMetaUpdateReason::CREATE_TABLET, out_reason);
  LOG_INFO("get_raw_meta succeed", K(out_row), K(out_head_value));

  // cleanup
  ASSERT_EQ(OB_SUCCESS, delete_tablet_meta_row(*meta_svr, tablet_key));
  LOG_INFO("[TEST CASE END] insert_and_get_raw_meta");
}

TEST_F(TestSharedMetaService, get_raw_meta_iter_basic)
{
  LOG_INFO("[TEST CASE BEGIN] get_raw_meta_iter_basic");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  // Synthetic ls_id (8004) to isolate from run_ctx_.ls_id_ (see comment in
  // insert_and_read_tablet_meta).
  const ObLSID ls_id(8004);
  const share::SCN reorganization_scn = share::SCN::min_scn();
  ObArenaAllocator allocator("TestRawIter");

  // tablet A (9900301): 3 versions at scn 100, 200, 300
  const ObTabletID tablet_a(9900301);
  const int64_t tablet_a_ver_count = 3;
  share::SCN tablet_a_scns[tablet_a_ver_count];
  for (int64_t i = 0; i < tablet_a_ver_count; i++) {
    tablet_a_scns[i] = share::SCN::plus(share::SCN::base_scn(), (i + 1) * 100);
  }

  // tablet B (9900302): 2 versions at scn 150, 250
  const ObTabletID tablet_b(9900302);
  const int64_t tablet_b_ver_count = 2;
  share::SCN tablet_b_scns[tablet_b_ver_count];
  tablet_b_scns[0] = share::SCN::plus(share::SCN::base_scn(), 150);
  tablet_b_scns[1] = share::SCN::plus(share::SCN::base_scn(), 250);

  // track all inserted keys for cleanup at the end
  ObSEArray<ObSSMetaTabletKey, 8> inserted_keys;

  // insert tablet A versions (meta_value is a serialized ObSSMetaHeadValue)
  for (int64_t i = 0; i < tablet_a_ver_count; i++) {
    ObSSMetaTabletKey key;
    key.ls_id_ = ls_id;
    key.tablet_id_ = tablet_a;
    key.reorganization_scn_ = reorganization_scn;
    key.meta_scn_ = tablet_a_scns[i];
    ASSERT_TRUE(key.is_valid());

    ObString value_str;
    ASSERT_EQ(OB_SUCCESS, build_head_value_str(allocator,
                                                ObMetaUpdateReason::CREATE_TABLET,
                                                value_str));
    ObSSMetaTabletRow row(static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META), key, value_str);
    ASSERT_EQ(OB_SUCCESS, meta_svr->insert_tablet_meta(row));
    ASSERT_EQ(OB_SUCCESS, inserted_keys.push_back(key));
  }

  // insert tablet B versions
  for (int64_t i = 0; i < tablet_b_ver_count; i++) {
    ObSSMetaTabletKey key;
    key.ls_id_ = ls_id;
    key.tablet_id_ = tablet_b;
    key.reorganization_scn_ = reorganization_scn;
    key.meta_scn_ = tablet_b_scns[i];
    ASSERT_TRUE(key.is_valid());

    ObString value_str;
    ASSERT_EQ(OB_SUCCESS, build_head_value_str(allocator,
                                                ObMetaUpdateReason::CREATE_TABLET,
                                                value_str));
    ObSSMetaTabletRow row(static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META), key, value_str);
    ASSERT_EQ(OB_SUCCESS, meta_svr->insert_tablet_meta(row));
    ASSERT_EQ(OB_SUCCESS, inserted_keys.push_back(key));
  }
  LOG_INFO("inserted tablet A (3 versions) and tablet B (2 versions)");

  // iterate tablet A with read_snapshot = base_scn + 250,
  // should only see versions with meta_scn <= 250 (i.e., scn 100 and 200)
  const share::SCN read_snapshot = share::SCN::plus(share::SCN::base_scn(), 250);
  ObSSMetaReadParamV2 read_param;
  read_param.set_tablet_level_param(
      ObSSMetaReadParamType::TABLET_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      ls_id, tablet_a, share::SCN::min_scn(), reorganization_scn,
      true /*is_in_table*/);
  read_param.set_read_mark_delete(true);

  ObSSMetaIterGuard<ObSSMetaRawRowIter> iter_guard;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_raw_meta_iter(read_param, read_snapshot, iter_guard));

  ObSSMetaRawRowIter *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
  ASSERT_NE(nullptr, iter);

  ObSSRawMetaRow out_row;
  int64_t count = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = iter->get_next(allocator, out_row))) {
    LOG_INFO("get_raw_meta_iter: got row", K(out_row));
    count++;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(2, count);
  LOG_INFO("get_raw_meta_iter_basic succeed", K(count));

  // cleanup
  for (int64_t i = 0; i < inserted_keys.count(); i++) {
    ASSERT_EQ(OB_SUCCESS, delete_tablet_meta_row(*meta_svr, inserted_keys.at(i)));
  }
  LOG_INFO("[TEST CASE END] get_raw_meta_iter_basic");
}

TEST_F(TestSharedMetaService, get_latest_raw_meta_iter)
{
  LOG_INFO("[TEST CASE BEGIN] get_latest_raw_meta_iter");
  const uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  const ObLSID ls_id(8006);
  const ObTabletID tablet_id(9900303);
  const share::SCN reorganization_scn = share::SCN::min_scn();
  const share::SCN live_scn = share::SCN::plus(
      share::SCN::base_scn(), 100);
  const share::SCN tombstone_scn = share::SCN::plus(
      share::SCN::base_scn(), 200);

  ObSSMetaTabletKey live_key;
  live_key.ls_id_ = ls_id;
  live_key.tablet_id_ = tablet_id;
  live_key.reorganization_scn_ = reorganization_scn;
  live_key.meta_scn_ = live_scn;
  ASSERT_TRUE(live_key.is_valid());
  ObSSMetaTabletKey tombstone_key = live_key;
  tombstone_key.meta_scn_ = tombstone_scn;
  ASSERT_TRUE(tombstone_key.is_valid());

  ObArenaAllocator value_allocator("LatestVisible");
  ObString live_value;
  ASSERT_EQ(OB_SUCCESS, build_head_value_str(value_allocator,
                                              ObMetaUpdateReason::CREATE_TABLET,
                                              live_value));
  ObSSMetaTabletRow live_row(
      static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META), live_key, live_value);
  ASSERT_EQ(OB_SUCCESS, meta_svr->insert_tablet_meta(live_row));

  ObString tombstone_value;
  ASSERT_EQ(OB_SUCCESS, build_head_value_str(value_allocator,
                                              ObMetaUpdateReason::CREATE_TABLET,
                                              tombstone_value,
                                              true /*mark_delete*/));
  ObSSMetaTabletRow tombstone_row(
      static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META), tombstone_key, tombstone_value);
  ASSERT_EQ(OB_SUCCESS, meta_svr->insert_tablet_meta(tombstone_row));

  ObSSMetaReadParamV2 read_param;
  read_param.set_tablet_level_param(
      ObSSMetaReadParamType::TABLET_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      ls_id, tablet_id, share::SCN::min_scn(), reorganization_scn,
      true /*is_in_table*/);

  // 删除标记可见前，返回最新的未删除版本。
  {
    ObSSMetaIterGuard<ObSSMetaRawRowIter> iter_guard;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_latest_raw_meta_iter(
        read_param, live_scn, iter_guard));

    ObSSMetaRawRowIter *iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
    ASSERT_NE(nullptr, iter);

    ObArenaAllocator row_allocator("LatestLiveRow");
    ObSSRawMetaRow row;
    ASSERT_EQ(OB_SUCCESS, iter->get_next(row_allocator, row));
    bool is_deleted = true;
    ASSERT_EQ(OB_SUCCESS, ObSSMetaHeadValue::check_mark_deleted(row.meta_value_, is_deleted));
    ASSERT_FALSE(is_deleted);
    ASSERT_EQ(OB_ITER_END, iter->get_next(row_allocator, row));
  }

  // 删除标记可见后，跳过删除标记时不能暴露更旧的未删除版本。
  {
    ObSSMetaIterGuard<ObSSMetaRawRowIter> iter_guard;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_latest_raw_meta_iter(
        read_param, tombstone_scn, iter_guard));
    ObSSMetaRawRowIter *iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
    ASSERT_NE(nullptr, iter);
    ObArenaAllocator row_allocator("SkipDeletedRow");
    ObSSRawMetaRow row;
    ASSERT_EQ(OB_ITER_END, iter->get_next(row_allocator, row));
  }

  // 读取删除标记时只返回最新版本，不同时返回两个版本。
  read_param.set_read_mark_delete(true);
  {
    ObSSMetaIterGuard<ObSSMetaRawRowIter> iter_guard;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_latest_raw_meta_iter(
        read_param, tombstone_scn, iter_guard));
    ObSSMetaRawRowIter *iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
    ASSERT_NE(nullptr, iter);
    ObArenaAllocator row_allocator("ReadDeletedRow");
    ObSSRawMetaRow row;
    ASSERT_EQ(OB_SUCCESS, iter->get_next(row_allocator, row));
    bool is_deleted = false;
    ASSERT_EQ(OB_SUCCESS, ObSSMetaHeadValue::check_mark_deleted(row.meta_value_, is_deleted));
    ASSERT_TRUE(is_deleted);
    ASSERT_EQ(OB_ITER_END, iter->get_next(row_allocator, row));
  }

  ASSERT_EQ(OB_SUCCESS, delete_tablet_meta_row(*meta_svr, tombstone_key));
  ASSERT_EQ(OB_SUCCESS, delete_tablet_meta_row(*meta_svr, live_key));
  LOG_INFO("[TEST CASE END] get_latest_raw_meta_iter");
}

TEST_F(TestSharedMetaService, delete_meta_row_basic)
{
  LOG_INFO("[TEST CASE BEGIN] delete_meta_row_basic");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  // Synthetic ls_id (8005) to isolate from run_ctx_.ls_id_ (see comment in
  // insert_and_read_tablet_meta).
  ObSSMetaTabletKey key;
  key.ls_id_ = ObLSID(8005);
  key.tablet_id_ = ObTabletID(9900201);
  key.reorganization_scn_ = share::SCN::min_scn();
  key.meta_scn_ = share::SCN::base_scn();
  ASSERT_TRUE(key.is_valid());

  // meta_value is a serialized ObSSMetaHeadValue (head-deserializable).
  ObArenaAllocator allocator("TestDelete");
  ObString value_str;
  ASSERT_EQ(OB_SUCCESS, build_head_value_str(allocator,
                                              ObMetaUpdateReason::CREATE_TABLET,
                                              value_str));
  ObSSMetaTabletRow row(static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META), key, value_str);
  ASSERT_EQ(OB_SUCCESS, meta_svr->insert_tablet_meta(row));

  // verify readable
  ObSSMetaTabletRow out_row;
  ASSERT_EQ(OB_SUCCESS, meta_svr->read_tablet_meta(key, allocator, out_row));

  // delete via delete_meta_row
  ObSSMetaDeleteParamV2 delete_param;
  delete_param.set_tablet_level_param(
      ObSSMetaReadParamType::TABLET_KEY,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      key.ls_id_, key.tablet_id_, key.meta_scn_, key.reorganization_scn_,
      true /*is_in_table*/);

  ASSERT_EQ(OB_SUCCESS, meta_svr->delete_meta_row(delete_param));
  LOG_INFO("delete_meta_row succeed", K(key), K(delete_param.affected_rows_));

  // verify deleted
  ObSSMetaTabletRow out_row2;
  int ret = meta_svr->read_tablet_meta(key, allocator, out_row2);
  ASSERT_NE(OB_SUCCESS, ret);
  LOG_INFO("delete_meta_row_basic: verified deletion", KR(ret));
  LOG_INFO("[TEST CASE END] delete_meta_row_basic");
}

// ====================== Category 3: High-level tablet interfaces ======================

TEST_F(TestSharedMetaService, create_and_get_tablet)
{
  LOG_INFO("[TEST CASE BEGIN] create_and_get_tablet");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id("test_svc2_table", tablet_id, ls_id));
  LOG_INFO("got tablet_id and ls_id", K(tablet_id), K(ls_id));

  run_ctx_.tablet_id_ = tablet_id;
  run_ctx_.ls_id_ = ls_id;

  // persist tablet meta to __all_shared_meta via create_tablet
  const share::SCN reorgan_scn = share::SCN::min_scn();
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id, tablet_id, reorgan_scn, true/*check_lease*/));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id, tablet_id, reorgan_scn, true/*check_lease*/));
  LOG_INFO("create_tablet succeed", K(ls_id), K(tablet_id));

  // get_tablet: 5-arg overload
  ObArenaAllocator allocator("TestGetTablet");
  ObTabletHandle tablet_hdl;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(ls_id, tablet_id, reorgan_scn,
                                              allocator, tablet_hdl));
  ASSERT_TRUE(tablet_hdl.is_valid());
  LOG_INFO("get_tablet (5-arg) succeed", K(ls_id), K(tablet_id));

  // get_tablet_latest_version
  share::SCN meta_scn;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_latest_version(ls_id, tablet_id,
                                                             reorgan_scn, meta_scn));
  ASSERT_TRUE(meta_scn.is_valid());
  LOG_INFO("get_tablet_latest_version succeed", K(meta_scn));
  LOG_INFO("[TEST CASE END] create_and_get_tablet");
}

TEST_F(TestSharedMetaService, transfer_upload_completion_retries_and_checks_lease)
{
  DDLFinishLeaseGuard lease_guard;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  ObTabletID unused_tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id("test_svc2_table", unused_tablet_id, ls_id));
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD));
  ObLS &ls = *ls_handle.get_ls();
  ObLSTabletService &tablet_svr = *ls.get_tablet_svr();
  ObArenaAllocator allocator("TransferUpload", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  uint64_t data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), data_version));
  share::schema::ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  table_schema.set_tenant_id(MTL_ID());
  ObCreateTabletSchema create_schema;
  ASSERT_EQ(OB_SUCCESS, create_schema.init(allocator, table_schema,
      lib::Worker::CompatMode::MYSQL, false /*skip_column_info*/, data_version));
  const SCN create_scn = SCN::base_scn();
  const SCN backfill_scn = SCN::plus(create_scn, 300);

  ObSEArray<ObTabletID, 2> tablet_ids;
  ObTabletHandle local_tablets[2];
  ObTabletBackfillInfo tablet_infos[2];
  DDLFinishTabletGuard tablet_guard(tablet_svr, tablet_ids);
  auto shared_meta_guard = MAKE_SCOPE(tablet_ids, [&](const ObIArray<ObTabletID> &ids) {
    for (int64_t i = 0; i < ids.count(); ++i) {
      ObSSMetaDeleteParamV2 param;
      param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_PREFIX,
          ObSSMetaReadResultType::READ_WHOLE_ROW, ObAtomicProtocolTypeEnum::TABLET_META,
          ls_id, ids.at(i), SCN::min_scn(), tablet_infos[i].reorganization_scn_, true /*is_in_table*/);
      const int ret = meta_svr->delete_meta_row(param);
      EXPECT_TRUE(OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret);
    }
  });
  auto commit_status = [&](const ObTabletID &tablet_id,
                           const ObTabletCreateDeleteMdsUserData &status, const SCN &scn) {
    mds::SingleLogMdsCtx ctx{mds::MdsWriter(ObTransID(tablet_id.id()))};
    ASSERT_EQ(OB_SUCCESS, ctx.init(ls_id, ObTimeUtility::current_time() + 10_s, false /*is_replay*/));
    bool committed = false;
    auto status_guard = MAKE_SCOPE(committed, [&](const bool &done) {
      if (!done) {
        EXPECT_EQ(OB_SUCCESS, ctx.get_mds_single_row_mvcc_ctx().trans_end(
            false, SCN::max_scn(), SCN::max_scn()));
        ctx.on_abort(SCN::max_scn());
      }
    });
    ASSERT_EQ(OB_SUCCESS, tablet_svr.set_tablet_status(tablet_id, status, ctx));
    ctx.single_log_commit(scn, scn);
    committed = true;
  };
  const int64_t first_tablet_id = ObTimeUtility::current_time();
  for (int64_t i = 0; i < 2; ++i) {
    const ObTabletID tablet_id(first_tablet_id + i);
    ASSERT_EQ(OB_SUCCESS, tablet_svr.create_tablet(ls_id, tablet_id, tablet_id, create_scn,
        1 /*snapshot_version*/, create_schema, lib::Worker::CompatMode::MYSQL,
        false /*need_create_empty_major_sstable*/, SCN::invalid_scn(),
        false /*micro_index_clustered*/, false /*has_cs_replica*/,
        ObSplitTabletInfo(), ObForkTabletInfo(), data_version, local_tablets[i]));
    ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id));
    ObTablet &local_tablet = *local_tablets[i].get_obj();
    ObTabletCreateDeleteMdsUserData status(
        ObTabletStatus::NORMAL, ObTabletMdsUserDataType::CREATE_TABLET, create_scn.get_val_for_tx());
    status.create_commit_scn_ = create_scn;
    ASSERT_NO_FATAL_FAILURE(commit_status(tablet_id, status, create_scn));
    const SCN reorgan_scn = local_tablet.get_reorganization_scn();
    ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id, tablet_id, reorgan_scn, true /*check_lease*/));

    local_tablet.table_store_cache_.is_async_index_refresh_target_ = true;

    status.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
    status.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_OUT;
    status.transfer_scn_ = backfill_scn;
    status.start_transfer_commit_scn_ = backfill_scn;
    status.start_transfer_commit_version_ = backfill_scn.get_val_for_tx();
    ASSERT_NO_FATAL_FAILURE(commit_status(tablet_id, status, backfill_scn));
    ObTabletBackfillInfo &info = tablet_infos[i];
    info.tablet_id_ = tablet_id;
    info.is_committed_ = true;
    info.is_shared_storage_ = true;
    info.relative_ls_id_ = ls_id;
    info.reorganization_scn_ = reorgan_scn;
    info.backfill_scn_ = backfill_scn;
    info.backfill_type_ = ObTabletBackfillType::BACKFILL_TRANSFER_OUT;
    info.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
    info.transfer_seq_ = local_tablet.get_tablet_meta().transfer_info_.transfer_seq_;
    ASSERT_TRUE(info.is_valid());
    ObTabletHandle checked_tablet;
    ASSERT_EQ(OB_SUCCESS, ObSSTransferBackfillUtils::get_and_check_source_tablet(ls, info, checked_tablet));
    ASSERT_TRUE(checked_tablet.get_obj()->is_async_index_refresh_target());
  }
  auto check_marker = [&](const int64_t i, const bool expected) {
    ObTabletHandle shared_tablet;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(ls_id, tablet_ids.at(i),
        tablet_infos[i].reorganization_scn_, allocator, shared_tablet));
    EXPECT_EQ(expected, shared_tablet.get_obj()->get_tablet_meta().transfer_info_.is_transfer_upload_completed());
    // Upload completion closes every ordinary update path, not just shared minor.
    const ObMetaUpdateReason ordinary_reasons[] = {
        ObMetaUpdateReason::TABLET_UPLOAD_DATA_MINI_SSTABLE,
        ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MINOR_SSTABLE,
        ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE,
        ObMetaUpdateReason::TABLET_UPLOAD_COMPACT_ADD_DATA_MINOR_SSTABLE,
        ObMetaUpdateReason::TABLET_UPLOAD_REPLACE_MINOR_SSTABLE,
        ObMetaUpdateReason::TABLET_UPLOAD_DDL_DUMP_SSTABLE,
        ObMetaUpdateReason::TABLET_UPLOAD_INC_MAJOR_DDL_DUMP_SSTABLE,
        ObMetaUpdateReason::TABLET_DDL_ADD_DATA_MAJOR_SSTABLE,
        ObMetaUpdateReason::TABLET_ADD_DATA_INC_MAJOR_SSTABLE,
        ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE};
    const ObMetaUpdateReason backfill_reasons[] = {
        ObMetaUpdateReason::TABLET_BACKFILL_ADD_SSTABLE,
        ObMetaUpdateReason::TABLET_BACKFILL_UPLOAD_SSTABLE,
        ObMetaUpdateReason::TABLET_TRANSFER_REPLACE_MINOR_SSTABLE,
        ObMetaUpdateReason::TABLET_BACKFILL_UPDATE_DATA_INC_MAJOR_SSTABLE};
    bool can_generate = false;
    bool can_gc = true;
    for (const auto reason : ordinary_reasons) {
      ASSERT_EQ(OB_SUCCESS, compaction::ObSSTabletMergeHelper::check_can_generate_new_tablet(
          ls_id, shared_tablet, reason, true /*need_check_split*/, can_generate, can_gc));
      EXPECT_EQ(!expected, can_generate) << static_cast<int64_t>(reason);
      EXPECT_FALSE(can_gc);
    }
    for (const auto reason : backfill_reasons) {
      ASSERT_EQ(OB_SUCCESS, compaction::ObSSTabletMergeHelper::check_can_generate_new_tablet(
          ls_id, shared_tablet, reason, true /*need_check_split*/, can_generate, can_gc));
      EXPECT_TRUE(can_generate) << static_cast<int64_t>(reason);
      EXPECT_FALSE(can_gc);
    }
  };
  ASSERT_NO_FATAL_FAILURE(check_marker(0, false));
  ASSERT_NO_FATAL_FAILURE(check_marker(1, false));

  ObStorageSchema *storage_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, local_tablets[0].get_obj()->load_storage_schema(allocator, storage_schema));
  DEFER(ObTabletObjLoadHelper::free(allocator, storage_schema));
  ObArray<ObSSMetaCtrlKeyInfo> empty_keys;
  auto create_empty_minor = [&](const SCN &start, const SCN &end,
                                const ObITable::TableType type, ObTableHandleV2 &handle) {
    ObTabletCreateSSTableParam param;
    ASSERT_EQ(OB_SUCCESS, param.init_for_transfer_empty_mini_minor_sstable(
        tablet_ids.at(0), start, end, *storage_schema, type));
    param.table_shared_flag_.set_shared_sstable();
    ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable(param, allocator, handle));
  };
  auto publish_minor = [&](ObTableHandleV2 &handle, const ObMetaUpdateReason reason,
                           const ObTablesHandleArray *inputs) {
    blocksstable::ObSSTable *sstable = nullptr;
    int ret = handle.get_sstable(sstable);
    if (OB_SUCCESS == ret) {
      ObUpdateTableStoreParam param(1, 1, storage_schema, 0, sstable);
      param.compaction_info_.merge_type_ = nullptr == inputs ? compaction::MINI_MERGE : compaction::MINOR_MERGE;
      param.compaction_info_.clog_checkpoint_scn_ = sstable->get_end_scn();
      param.input_tables_handle_ = inputs;
      ret = meta_svr->update_tablet_table_store(ls_id, tablet_ids.at(0),
          tablet_infos[0].reorganization_scn_, reason, param, empty_keys, true /*check_lease*/);
    }
    return ret;
  };
  // Reproduce the 63 -> 64 upload overflow while the source is TRANSFER_OUT.
  // Ordinary shared minor must reclaim slots before upload can finish.
  ObTablesHandleArray minor_inputs;
  const int64_t full_minor_count = MAX_SSTABLE_CNT_IN_STORAGE - 1;
  for (int64_t i = 0; i < full_minor_count; ++i) {
    ObTableHandleV2 mini;
    ASSERT_NO_FATAL_FAILURE(create_empty_minor(SCN::plus(create_scn, i), SCN::plus(create_scn, i + 1),
        ObITable::MINI_SSTABLE, mini));
    ASSERT_EQ(OB_SUCCESS, publish_minor(mini, ObMetaUpdateReason::TABLET_BACKFILL_UPLOAD_SSTABLE, nullptr));
    ASSERT_EQ(OB_SUCCESS, minor_inputs.add_table(mini));
  }
  ObTableHandleV2 pending_mini;
  ASSERT_NO_FATAL_FAILURE(create_empty_minor(SCN::plus(create_scn, full_minor_count),
      SCN::plus(create_scn, full_minor_count + 1), ObITable::MINI_SSTABLE, pending_mini));
  ASSERT_EQ(OB_SIZE_OVERFLOW, publish_minor(pending_mini,
      ObMetaUpdateReason::TABLET_BACKFILL_UPLOAD_SSTABLE, nullptr));
  ObTableHandleV2 merged_minor;
  ASSERT_NO_FATAL_FAILURE(create_empty_minor(create_scn, SCN::plus(create_scn, full_minor_count),
      ObITable::MINOR_SSTABLE, merged_minor));
  ASSERT_EQ(OB_SUCCESS, publish_minor(merged_minor,
      ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MINOR_SSTABLE, &minor_inputs));
  ASSERT_EQ(OB_SUCCESS, publish_minor(pending_mini,
      ObMetaUpdateReason::TABLET_BACKFILL_UPLOAD_SSTABLE, nullptr));
  ObTabletHandle compacted_tablet;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(ls_id, tablet_ids.at(0),
      tablet_infos[0].reorganization_scn_, allocator, compacted_tablet));
  EXPECT_EQ(2, compacted_tablet.get_obj()->get_minor_table_count());
  EXPECT_FALSE(compacted_tablet.get_obj()->get_tablet_meta().transfer_info_.is_transfer_upload_completed());

  // A normal major can also publish before upload completes. Completing upload
  // must preserve that table store; a later publication must see the new marker.
  ObTabletCreateSSTableParam create_param;
  ASSERT_EQ(OB_SUCCESS, create_param.init_for_empty_major_sstable(tablet_ids.at(0), *storage_schema,
      1 /*snapshot_version*/, -1 /*column_group_idx*/, false /*has_all_column_group*/, true /*is_shared*/));
  ObTableHandleV2 major_handle;
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable(create_param, allocator, major_handle));
  blocksstable::ObSSTable *major = nullptr;
  ASSERT_EQ(OB_SUCCESS, major_handle.get_sstable(major));
  ObUpdateTableStoreParam update_param(1, 1, storage_schema, 0, major);
  update_param.compaction_info_.merge_type_ = compaction::MAJOR_MERGE;
  ASSERT_TRUE(update_param.is_valid());
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_tablet_table_store(ls_id, tablet_ids.at(0),
      tablet_infos[0].reorganization_scn_, ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE,
      update_param, empty_keys, true /*check_lease*/));

  // Model a writer that already passed its check on upload=false, then loses
  // the publication race to finish_upload_. Its old op_id must not commit.
  ObTabletAtomicProtocol stale_update;
  ObTabletAtomicProtocolParam protocol_param;
  ASSERT_EQ(OB_SUCCESS, meta_svr->build_tablet_protocol_init_param_(
      ls_id, tablet_ids.at(0), tablet_infos[0].reorganization_scn_, empty_keys, protocol_param));
  ASSERT_EQ(OB_SUCCESS, stale_update.init(protocol_param));
  ASSERT_EQ(OB_SUCCESS, stale_update.create(true /*need_check_sswriter*/));
  DEFER(EXPECT_EQ(OB_SUCCESS, stale_update.abort_self()));
  ObTabletHandle before_upload;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(ls_id, tablet_ids.at(0),
      tablet_infos[0].reorganization_scn_, allocator, before_upload));
  ASSERT_FALSE(before_upload.get_obj()->get_tablet_meta().transfer_info_.is_transfer_upload_completed());

  ObSSTransferBackfillTXCtx ctx;
  ctx.backfill_type_ = ObSSTransferBackfillType::BACKFILL_UPLOAD;
  ctx.src_ls_handle_ = ls_handle;
  ASSERT_EQ(OB_SUCCESS, ctx.tablet_infos_.push_back(tablet_infos[0]));
  ASSERT_EQ(OB_SUCCESS, ctx.tablet_infos_.push_back(tablet_infos[1]));
  // A missing second source generation must not undo completion already persisted for the first one.
  ctx.tablet_infos_.at(1).reorganization_scn_ = SCN::plus(tablet_infos[1].reorganization_scn_, 1);
  ObSSFinishTransferBackfillDag dag;
  ObSSFinishTransferBackfillTask task;
  task.set_dag(dag);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, task.finish_upload_(ctx));
  ASSERT_NO_FATAL_FAILURE(check_marker(0, true));
  ASSERT_NO_FATAL_FAILURE(check_marker(1, false));
  class StalePublishOP : public ObIAtomicProtocolOP
  {
  public:
    int process(common::ObMySQLTransaction &, const share::SCN &) override
    {
      called_ = true;
      return OB_SUCCESS;
    }
    bool called_ = false;
  } stale_op;
  ASSERT_EQ(OB_NEED_RETRY, stale_update.finish(stale_op));
  EXPECT_FALSE(stale_op.called_);
  EXPECT_FALSE(before_upload.get_obj()->get_tablet_meta().transfer_info_.is_transfer_upload_completed());
  ASSERT_EQ(OB_OP_NOT_ALLOW, meta_svr->update_tablet_table_store(ls_id, tablet_ids.at(0),
      tablet_infos[0].reorganization_scn_, ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE,
      update_param, empty_keys, true /*check_lease*/));
  ObTabletHandle after_upload;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(ls_id, tablet_ids.at(0),
      tablet_infos[0].reorganization_scn_, allocator, after_upload));
  EXPECT_TRUE(after_upload.get_obj()->get_tablet_meta().transfer_info_.is_transfer_upload_completed());
  EXPECT_EQ(1, after_upload.get_obj()->get_major_table_count());
  ctx.tablet_infos_.at(1).reorganization_scn_ = tablet_infos[1].reorganization_scn_;

  // Keep ctx successful: cancellation itself must prevent a new persisted marker.
  class CanceledUploadDagNet : public ObSSTransferBackfillDagNet
  {
  public:
    int deal_with_cancel() override { return OB_SUCCESS; }
  } canceled_net;
  ASSERT_EQ(OB_SUCCESS, canceled_net.set_cancel());
  dag.set_dag_net(canceled_net);
  ASSERT_FALSE(ctx.is_failed());
  ASSERT_EQ(OB_SUCCESS, task.finish_upload_(ctx));
  ASSERT_NO_FATAL_FAILURE(check_marker(0, true));
  ASSERT_NO_FATAL_FAILURE(check_marker(1, false));
  dag.clear_dag_net();

  // Pass the outer FREEZE check, then reject the lease inside update_tablet_meta.
  check_lease_call_count = 0;
  ddl_lease_reject_at = 2;
  ASSERT_EQ(OB_NOT_MASTER, task.finish_upload_(ctx));
  ASSERT_GE(check_lease_call_count, 2);
  ASSERT_NO_FATAL_FAILURE(check_marker(0, true));
  ASSERT_NO_FATAL_FAILURE(check_marker(1, false));
  ddl_lease_reject_at = 0;
  ASSERT_EQ(OB_SUCCESS, task.finish_upload_(ctx));
  ASSERT_NO_FATAL_FAILURE(check_marker(0, true));
  ASSERT_NO_FATAL_FAILURE(check_marker(1, true));
  ASSERT_EQ(OB_SUCCESS, task.finish_upload_(ctx));
}

TEST_F(TestSharedMetaService, atomic_protocol_writer_qualification_follows_tenant_role)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(run_ctx_.tenant_id_));
  ObSSWriterService *service = MTL(ObSSWriterService *);
  ASSERT_NE(nullptr, service);

  // Replace only this thread's tenant context to preserve the tenant role seen by background threads.
  share::ObTenantBase *saved_tenant = share::ObTenantEnv::get_tenant();
  share::ObTenantBase test_tenant(saved_tenant->id());
  test_tenant = *saved_tenant;
  auto tenant_guard = MAKE_SCOPE(saved_tenant, [](share::ObTenantBase *tenant) {
    share::ObTenantEnv::set_tenant(tenant);
  });
  share::ObTenantEnv::set_tenant(&test_tenant);

  const bool saved_sswriter_enabled = ATOMIC_LOAD(&mock_sswriter_enabled);
  const bool saved_replay_writer = mock_replay_writer;
  const bool saved_is_sswriter = mock_is_sswriter;
  auto lease_guard = MAKE_SCOPE(saved_sswriter_enabled, [&](const bool &enabled) {
    ATOMIC_STORE(&mock_sswriter_enabled, enabled);
    mock_replay_writer = saved_replay_writer;
    mock_is_sswriter = saved_is_sswriter;
  });
  ATOMIC_STORE(&mock_sswriter_enabled, false);
  mock_replay_writer = true;
  mock_is_sswriter = true;
  const ObSSWriterKey key(ObSSWriterType::META, SYS_LS, ObTabletID(9900902));
  bool is_sswriter = false;
  int64_t epoch = 0;
  for (const auto role : {share::ObTenantRole::PRIMARY_TENANT, share::ObTenantRole::INVALID_TENANT}) {
    test_tenant.set_tenant_role(role);
    ASSERT_EQ(OB_SUCCESS, service->check_lease_for_atomic_protocol(key, is_sswriter, epoch));
    EXPECT_TRUE(is_sswriter);
    EXPECT_EQ(ATOMIC_LOAD(&lease_epoch), epoch);
  }
  for (const auto role : {share::ObTenantRole::STANDBY_TENANT, share::ObTenantRole::RESTORE_TENANT}) {
    test_tenant.set_tenant_role(role);
    ATOMIC_STORE(&mock_sswriter_enabled, false);
    ASSERT_EQ(OB_SUCCESS, service->check_lease_for_atomic_protocol(key, is_sswriter, epoch));
    EXPECT_FALSE(is_sswriter);
    ATOMIC_STORE(&mock_sswriter_enabled, true);
    ASSERT_EQ(OB_SUCCESS, service->check_lease_for_atomic_protocol(key, is_sswriter, epoch));
    EXPECT_TRUE(is_sswriter);
    EXPECT_EQ(ATOMIC_LOAD(&lease_epoch), epoch);
  }
}

TEST_F(TestSharedMetaService, ddl_finish_replay_rebuilds_shared_tablet)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(run_ctx_.tenant_id_));
  ObTabletID source_tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id("test_svc2_table", source_tablet_id, ls_id));
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  const ObTabletID tablet_id(9900901);
  ObArenaAllocator allocator("DDLReplayLease");
  share::schema::ObTableSchema data_schema;
  share::schema::ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(data_schema);
  TestSchemaUtils::prepare_index_schema(table_schema);
  table_schema.set_tenant_id(run_ctx_.tenant_id_);
  table_schema.set_data_table_id(data_schema.get_table_id());
  table_schema.set_schema_version(data_schema.get_schema_version());
  table_schema.set_table_type(share::schema::USER_INDEX);
  table_schema.set_index_type(share::schema::INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_index_status(share::schema::INDEX_STATUS_UNAVAILABLE);
  uint64_t data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(run_ctx_.tenant_id_, data_version));
  ObCreateTabletSchema create_schema;
  ASSERT_EQ(OB_SUCCESS, create_schema.init(
      allocator, table_schema, lib::Worker::CompatMode::MYSQL, false/*skip_column_info*/, data_version));
  share::SCN create_scn;
  ASSERT_EQ(OB_SUCCESS, create_scn.convert_from_ts(ObTimeUtility::current_time()));

  const bool saved_sswriter_enabled = ATOMIC_LOAD(&mock_sswriter_enabled);
  const bool saved_replay_writer = mock_replay_writer;
  auto lease_guard = MAKE_SCOPE(saved_sswriter_enabled, [&](const bool &enabled) {
    ATOMIC_STORE(&mock_sswriter_enabled, enabled);
    mock_replay_writer = saved_replay_writer;
  });
  // Grant mock writer qualification only to the replay thread, so background uploads cannot rebuild the shared tablet.
  ATOMIC_STORE(&mock_sswriter_enabled, false);
  mock_replay_writer = true;
  ObTabletHandle local_tablet;
  // An unfinished index has no local major. An empty major from normal table creation would skip the replay branch.
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet_svr()->create_tablet(
      ls_id, tablet_id, tablet_id, create_scn, 1/*snapshot_version*/, create_schema,
      lib::Worker::CompatMode::MYSQL, false/*need_create_empty_major_sstable*/,
      share::SCN::invalid_scn()/*clog_checkpoint_scn*/, false/*micro_index_clustered*/, false/*has_cs_replica*/,
      share::ObSplitTabletInfo(), share::ObForkTabletInfo(), data_version, local_tablet));
  const share::SCN reorgan_scn = local_tablet.get_obj()->get_reorganization_scn();
  auto tablet_guard = MAKE_SCOPE(tablet_id, [&](const ObTabletID &id) {
    ObTabletHandle latest_tablet;
    const int get_ret = ls->get_tablet_svr()->get_tablet(
        id, latest_tablet, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
    EXPECT_TRUE(OB_SUCCESS == get_ret || OB_TABLET_NOT_EXIST == get_ret);
    if (OB_SUCCESS == get_ret) {
      EXPECT_EQ(OB_SUCCESS, ls->get_tablet_svr()->remove_tablet(latest_tablet));
    }
    ObSSMetaDeleteParamV2 delete_param;
    delete_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX, ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META, ls_id, id, share::SCN::min_scn(),
        reorgan_scn, true/*is_in_table*/);
    const int delete_ret = meta_svr->delete_meta_row(delete_param);
    EXPECT_TRUE(OB_SUCCESS == delete_ret || OB_ENTRY_NOT_EXIST == delete_ret);
  });
  ASSERT_TRUE(local_tablet.get_obj()->get_tablet_meta().is_valid());
  {
    mds::SingleLogMdsCtx status_ctx{mds::MdsWriter(ObTransID(tablet_id.id()))};
    ASSERT_EQ(OB_SUCCESS, status_ctx.init(
        ls_id, ObTimeUtility::current_time() + 10_s, false/*is_replay*/));
    bool committed = false;
    auto status_guard = MAKE_SCOPE(status_ctx, [&](mds::SingleLogMdsCtx &ctx) {
      if (!committed) {
        EXPECT_EQ(OB_SUCCESS, ctx.get_mds_single_row_mvcc_ctx().trans_end(
            false, share::SCN::max_scn(), share::SCN::max_scn()));
        ctx.on_abort(share::SCN::max_scn());
      }
    });
    ObTabletCreateDeleteMdsUserData status(
        ObTabletStatus::NORMAL, ObTabletMdsUserDataType::CREATE_TABLET, create_scn.get_val_for_tx());
    status.create_commit_scn_ = create_scn;
    ASSERT_EQ(OB_SUCCESS, ls->get_tablet_svr()->set_tablet_status(tablet_id, status, status_ctx));
    status_ctx.single_log_commit(create_scn, create_scn);
    committed = true;
  }
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet_svr()->get_tablet(tablet_id, local_tablet));
  ASSERT_EQ(0, local_tablet.get_obj()->get_major_table_count());
  ASSERT_FALSE(local_tablet.get_obj()->get_tablet_meta().table_store_flag_.with_major_sstable());
  ObTabletHandle shared_tablet;
  ASSERT_EQ(OB_TABLET_NOT_EXIST, meta_svr->get_tablet(
      ls_id, tablet_id, reorgan_scn, allocator, shared_tablet));

  const int64_t snapshot_version = 100;
  ObTabletCreateSSTableParam sstable_param;
  ASSERT_EQ(OB_SUCCESS, sstable_param.init_for_empty_major_sstable(
      tablet_id, create_schema, snapshot_version, -1/*column_group_idx*/,
      false/*has_all_column_group*/, true/*is_shared*/));
  ObSSTable major;
  ASSERT_EQ(OB_SUCCESS, major.init(sstable_param, &allocator));
  const int64_t buf_len = major.get_serialize_size(data_version);
  char *buf = static_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, major.serialize(data_version, buf, buf_len, pos));
  ObDDLFinishLog finish_log;
  ASSERT_EQ(OB_SUCCESS, finish_log.init(
      run_ctx_.tenant_id_, ls_id, major.get_key(), buf, pos, data_version));
  const share::SCN replay_scn = share::SCN::scn_inc(create_scn);
  ASSERT_GT(replay_scn, local_tablet.get_obj()->get_tablet_meta().ddl_checkpoint_scn_);
  ObDDLFinishReplayExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.init(ls, finish_log, replay_scn));
  check_lease_call_count = 0;
  ASSERT_EQ(OB_SUCCESS, executor.execute(replay_scn, ls_id, tablet_id));
  EXPECT_GT(check_lease_call_count, 0);
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(
      ls_id, tablet_id, reorgan_scn, allocator, shared_tablet));
  ASSERT_EQ(snapshot_version, shared_tablet.get_obj()->get_last_compaction_scn());
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet_svr()->get_tablet(tablet_id, local_tablet));
  ASSERT_EQ(1, local_tablet.get_obj()->get_major_table_count());
  ASSERT_EQ(snapshot_version, local_tablet.get_obj()->get_last_major_snapshot_version());

  share::SCN meta_scn_before;
  share::SCN meta_scn_after;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_latest_version(
      ls_id, tablet_id, reorgan_scn, meta_scn_before));
  check_lease_call_count = 0;
  ASSERT_EQ(OB_SUCCESS, executor.execute(replay_scn, ls_id, tablet_id));
  EXPECT_EQ(0, check_lease_call_count);
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_latest_version(
      ls_id, tablet_id, reorgan_scn, meta_scn_after));
  EXPECT_EQ(meta_scn_before, meta_scn_after);
}

TEST_F(TestSharedMetaService, get_tablet_with_meta_scn)
{
  LOG_INFO("[TEST CASE BEGIN] get_tablet_with_meta_scn");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  ObTabletID tablet_id = run_ctx_.tablet_id_;
  ObLSID ls_id = run_ctx_.ls_id_;
  ASSERT_TRUE(tablet_id.is_valid());
  ASSERT_TRUE(ls_id.is_valid());

  // get_tablet: 6-arg overload that returns meta_scn
  ObArenaAllocator allocator("TestGetTablet2");
  ObTabletHandle tablet_hdl;
  share::SCN meta_scn;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(ls_id, tablet_id, share::SCN::min_scn(),
                                              allocator, tablet_hdl, meta_scn));
  ASSERT_TRUE(tablet_hdl.is_valid());
  ASSERT_TRUE(meta_scn.is_valid());
  LOG_INFO("get_tablet_with_meta_scn succeed", K(ls_id), K(tablet_id), K(meta_scn));
  LOG_INFO("[TEST CASE END] get_tablet_with_meta_scn");
}

TEST_F(TestSharedMetaService, get_ls_meta_by_exact_scn)
{
  LOG_INFO("[TEST CASE BEGIN] get_ls_meta_by_exact_scn");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  const ObLSID ls_id(1060);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg, ObSSLSCreateMode::NORMAL_CREATE));

  // 1. get meta_scn via scn iter
  ObSSMetaReadParamV2 read_param;
  read_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::LS_META,
      ls_id,
      share::SCN::min_scn(),
      true /*is_in_table*/);

  share::SCN meta_scn;
  {
    ObSSMetaIterGuard<ObSSMetaLSSCNIterator> iter_guard;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_scn_iter(read_param, share::SCN::max_scn(), iter_guard));
    ObSSMetaLSSCNIterator *iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
    ASSERT_EQ(OB_SUCCESS, iter->get_next(meta_scn));
    ASSERT_TRUE(meta_scn.is_valid());
  }
  LOG_INFO("get_ls_meta_by_exact_scn: got meta_scn", K(ls_id), K(meta_scn));

  // 2. get_ls_meta with exact meta_scn should succeed
  ObSSLSMeta ls_meta;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(ls_id, meta_scn, ls_meta));
  ASSERT_TRUE(ls_meta.is_valid());
  LOG_INFO("get_ls_meta by exact scn succeed", K(ls_id), K(meta_scn), K(ls_meta));

  // 3. get_ls_meta with a non-existent meta_scn should fail
  share::SCN fake_scn;
  fake_scn.convert_for_logservice(1);
  ObSSLSMeta ls_meta2;
  int ret = meta_svr->get_ls_meta(ls_id, fake_scn, ls_meta2);
  ASSERT_EQ(OB_LS_NOT_EXIST, ret);
  LOG_INFO("get_ls_meta with fake scn correctly returned OB_LS_NOT_EXIST");

  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
  LOG_INFO("[TEST CASE END] get_ls_meta_by_exact_scn");
}

TEST_F(TestSharedMetaService, get_tablet_by_exact_scn)
{
  LOG_INFO("[TEST CASE BEGIN] get_tablet_by_exact_scn");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  ObTabletID tablet_id = run_ctx_.tablet_id_;
  ObLSID ls_id = run_ctx_.ls_id_;
  ASSERT_TRUE(tablet_id.is_valid());
  ASSERT_TRUE(ls_id.is_valid());

  const share::SCN reorgan_scn = share::SCN::min_scn();

  // 1. get meta_scn via get_tablet_latest_version
  share::SCN meta_scn;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_latest_version(ls_id, tablet_id,
                                                             reorgan_scn, meta_scn));
  ASSERT_TRUE(meta_scn.is_valid());
  LOG_INFO("get_tablet_by_exact_scn: got meta_scn", K(ls_id), K(tablet_id), K(meta_scn));

  // 2. get_tablet with exact meta_scn should succeed
  ObArenaAllocator allocator("TestExactTab");
  ObTabletHandle tablet_hdl;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(ls_id, tablet_id, reorgan_scn,
                                              meta_scn, allocator, tablet_hdl));
  ASSERT_TRUE(tablet_hdl.is_valid());
  LOG_INFO("get_tablet by exact scn succeed", K(ls_id), K(tablet_id), K(meta_scn));

  // 3. get_tablet with a non-existent meta_scn should fail
  share::SCN fake_scn;
  fake_scn.convert_for_logservice(1);
  ObArenaAllocator allocator2("TestExactTab2");
  ObTabletHandle tablet_hdl2;
  int ret = meta_svr->get_tablet(ls_id, tablet_id, reorgan_scn,
                                 fake_scn, allocator2, tablet_hdl2);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  LOG_INFO("get_tablet with fake scn correctly returned OB_TABLET_NOT_EXIST");

  LOG_INFO("[TEST CASE END] get_tablet_by_exact_scn");
}

TEST_F(TestSharedMetaService, get_tablet_id_iter_basic)
{
  LOG_INFO("[TEST CASE BEGIN] get_tablet_id_iter_basic");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  ObLSID ls_id = run_ctx_.ls_id_;
  ASSERT_TRUE(ls_id.is_valid());

  ObSSMetaReadParamV2 read_param;
  read_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      ls_id,
      share::SCN::min_scn(),
      true /*is_in_table*/);

  ObSSMetaIterGuard<ObSSMetaTabletKeyIterator> iter_guard;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_id_iter(read_param, share::SCN::max_scn(), iter_guard));

  ObSSMetaTabletKeyIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
  ASSERT_NE(nullptr, iter);

  ObSSMetaTabletKey tablet_key;
  int64_t count = 0;
  bool found = false;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = iter->get_next(tablet_key))) {
    LOG_INFO("get_tablet_id_iter: got key", K(tablet_key));
    if (tablet_key.tablet_id_ == run_ctx_.tablet_id_) {
      found = true;
    }
    count++;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GE(count, 1);
  ASSERT_TRUE(found);
  LOG_INFO("get_tablet_id_iter_basic succeed", K(count));
  LOG_INFO("[TEST CASE END] get_tablet_id_iter_basic");
}

TEST_F(TestSharedMetaService, get_tablet_id_iter_with_mark_delete)
{
  LOG_INFO("[TEST CASE BEGIN] get_tablet_id_iter_with_mark_delete");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  // create a dedicated tablet, then mark it deleted
  ASSERT_EQ(OB_SUCCESS, exe_sql("create table test_svc2_iter_mark_delete (a int)"));
  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS,
            get_tablet_and_ls_id("test_svc2_iter_mark_delete", tablet_id, ls_id));
  LOG_INFO("got tablet_id and ls_id for iter mark_delete", K(tablet_id), K(ls_id));

  const share::SCN reorgan_scn = share::SCN::min_scn();
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id, tablet_id, reorgan_scn, true/*check_lease*/));
  ASSERT_EQ(OB_SUCCESS, meta_svr->mark_tablet_deleted(
      ls_id, tablet_id, reorgan_scn, share::SCN::base_scn()));
  LOG_INFO("create_tablet and mark_tablet_deleted succeed", K(tablet_id));

  // with set_read_mark_delete(true), the mark_deleted tablet must still be iterated
  ObSSMetaReadParamV2 read_param;
  read_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      ls_id,
      share::SCN::min_scn(),
      true /*is_in_table*/);
  read_param.set_read_mark_delete(true);

  ObSSMetaIterGuard<ObSSMetaTabletKeyIterator> iter_guard;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_id_iter(read_param, share::SCN::max_scn(), iter_guard));

  ObSSMetaTabletKeyIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
  ASSERT_NE(nullptr, iter);

  ObSSMetaTabletKey tablet_key;
  bool found = false;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = iter->get_next(tablet_key))) {
    LOG_INFO("get_tablet_id_iter(read_mark_delete): got key", K(tablet_key));
    if (tablet_key.tablet_id_ == tablet_id) {
      found = true;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_TRUE(found);
  LOG_INFO("get_tablet_id_iter_with_mark_delete succeed", K(tablet_id));
  LOG_INFO("[TEST CASE END] get_tablet_id_iter_with_mark_delete");
}

TEST_F(TestSharedMetaService, get_tablet_id_iter_skip_mark_delete)
{
  LOG_INFO("[TEST CASE BEGIN] get_tablet_id_iter_skip_mark_delete");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  // create a dedicated tablet, then mark it deleted
  ASSERT_EQ(OB_SUCCESS, exe_sql("create table test_svc2_iter_skip_mark_delete (a int)"));
  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS,
            get_tablet_and_ls_id("test_svc2_iter_skip_mark_delete", tablet_id, ls_id));
  LOG_INFO("got tablet_id and ls_id for iter skip mark_delete", K(tablet_id), K(ls_id));

  const share::SCN reorgan_scn = share::SCN::min_scn();
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id, tablet_id, reorgan_scn, true/*check_lease*/));
  ASSERT_EQ(OB_SUCCESS, meta_svr->mark_tablet_deleted(
      ls_id, tablet_id, reorgan_scn, share::SCN::base_scn()));
  LOG_INFO("create_tablet and mark_tablet_deleted succeed", K(tablet_id));

  // without set_read_mark_delete (default false), the mark_deleted tablet must be skipped
  ObSSMetaReadParamV2 read_param;
  read_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      ls_id,
      share::SCN::min_scn(),
      true /*is_in_table*/);

  ObSSMetaIterGuard<ObSSMetaTabletKeyIterator> iter_guard;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_id_iter(read_param, share::SCN::max_scn(), iter_guard));

  ObSSMetaTabletKeyIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
  ASSERT_NE(nullptr, iter);

  ObSSMetaTabletKey tablet_key;
  bool found = false;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = iter->get_next(tablet_key))) {
    LOG_INFO("get_tablet_id_iter(skip_mark_delete): got key", K(tablet_key));
    if (tablet_key.tablet_id_ == tablet_id) {
      found = true;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_FALSE(found);
  LOG_INFO("get_tablet_id_iter_skip_mark_delete succeed", K(tablet_id));
  LOG_INFO("[TEST CASE END] get_tablet_id_iter_skip_mark_delete");
}

TEST_F(TestSharedMetaService, get_tablet_key_iter_basic)
{
  LOG_INFO("[TEST CASE BEGIN] get_tablet_key_iter_basic");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  ObLSID ls_id = run_ctx_.ls_id_;
  ASSERT_TRUE(ls_id.is_valid());

  ObSSMetaReadParamV2 read_param;
  read_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      ls_id,
      share::SCN::min_scn(),
      true /*is_in_table*/);

  ObSSMetaIterGuard<ObSSMetaTabletKeyIterator> iter_guard;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_key_iter(read_param, share::SCN::max_scn(), iter_guard));

  ObSSMetaTabletKeyIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
  ASSERT_NE(nullptr, iter);

  ObSSMetaTabletKey tablet_key;
  int64_t count = 0;
  bool found = false;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = iter->get_next(tablet_key))) {
    LOG_INFO("get_tablet_key_iter: got key", K(tablet_key));
    if (tablet_key.tablet_id_ == run_ctx_.tablet_id_) {
      found = true;
      ASSERT_TRUE(tablet_key.meta_scn_.is_valid());
    }
    count++;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GE(count, 1);
  ASSERT_TRUE(found);
  LOG_INFO("get_tablet_key_iter_basic succeed", K(count));
  LOG_INFO("[TEST CASE END] get_tablet_key_iter_basic");
}

TEST_F(TestSharedMetaService, get_tablet_scn_iter_basic)
{
  LOG_INFO("[TEST CASE BEGIN] get_tablet_scn_iter_basic");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  ObLSID ls_id = run_ctx_.ls_id_;
  ObTabletID tablet_id = run_ctx_.tablet_id_;
  ASSERT_TRUE(ls_id.is_valid());
  ASSERT_TRUE(tablet_id.is_valid());

  ObSSMetaReadParamV2 read_param;
  read_param.set_tablet_level_param(
      ObSSMetaReadParamType::TABLET_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      ls_id, tablet_id, share::SCN::min_scn(), share::SCN::min_scn(),
      true /*is_in_table*/);

  ObSSMetaIterGuard<ObSSMetaTabletSCNIterator> iter_guard;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_scn_iter(read_param, share::SCN::max_scn(), iter_guard));

  ObSSMetaTabletSCNIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
  ASSERT_NE(nullptr, iter);

  share::SCN scn;
  int64_t count = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = iter->get_next(scn))) {
    ASSERT_TRUE(scn.is_valid());
    LOG_INFO("get_tablet_scn_iter: got scn", K(scn));
    count++;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GE(count, 1);
  LOG_INFO("get_tablet_scn_iter_basic succeed", K(count));
  LOG_INFO("[TEST CASE END] get_tablet_scn_iter_basic");
}

TEST_F(TestSharedMetaService, get_tablet_iter_basic)
{
  LOG_INFO("[TEST CASE BEGIN] get_tablet_iter_basic");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  ObLSID ls_id = run_ctx_.ls_id_;
  ASSERT_TRUE(ls_id.is_valid());

  ObSSMetaReadParamV2 read_param;
  read_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      ls_id,
      share::SCN::min_scn(),
      true /*is_in_table*/);
  read_param.set_read_mark_delete(true);

  ObSSMetaIterGuard<ObSSMetaTabletIter> iter_guard;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_iter(read_param, share::SCN::max_scn(), iter_guard));

  ObSSMetaTabletIter *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
  ASSERT_NE(nullptr, iter);

  ObArenaAllocator allocator("TestTabletIter");
  ObTabletHandle tablet_handle;
  share::SCN reorganization_scn;
  share::SCN meta_version;
  int64_t count = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = iter->get_next(allocator, tablet_handle,
                                             reorganization_scn, meta_version))) {
    ASSERT_TRUE(tablet_handle.is_valid());
    ASSERT_TRUE(meta_version.is_valid());
    LOG_INFO("get_tablet_iter: got tablet", K(reorganization_scn), K(meta_version));
    count++;
    tablet_handle.reset();
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GE(count, 1);
  LOG_INFO("get_tablet_iter_basic succeed", K(count));
  LOG_INFO("[TEST CASE END] get_tablet_iter_basic");
}

// Covers the TABLET_PREFIX branch of ObSSMetaService::get_tablet_iter, where
// the read param is tablet-level (not LS_PREFIX) so the iterator is initialized
// for a single tablet instead of scanning the whole LS.
//
// Two sub-cases are exercised:
//   1) reorgan_scn is valid  -> prefix is built down to the reorganization_scn
//      level, so only that single (tablet_id, reorgan_scn) version is returned.
//   2) reorgan_scn is invalid -> prefix is only built down to the tablet_id
//      level (see ObSSMetaInfoUtil::gen_tablet_meta_key_prefix), so the iterator
//      returns the latest version of EACH reorganization_scn of the same tablet.
TEST_F(TestSharedMetaService, get_tablet_iter_tablet_prefix)
{
  LOG_INFO("[TEST CASE BEGIN] get_tablet_iter_tablet_prefix");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  // run_ctx_.{ls_id_,tablet_id_} are populated by the create_and_get_tablet
  // case (which runs earlier) and point at a fully serialized tablet meta row
  // at reorganization_scn = min_scn.
  ObLSID ls_id = run_ctx_.ls_id_;
  ObTabletID tablet_id = run_ctx_.tablet_id_;
  ASSERT_TRUE(ls_id.is_valid());
  ASSERT_TRUE(tablet_id.is_valid());

  // Persist a second version of the SAME tablet under a different
  // reorganization_scn so the invalid-reorgan_scn path has >1 row to iterate.
  // NOTE: create_tablet() cannot be used here. It enforces
  //   local_tablet.reorganization_scn == requested reorgan_scn
  // (see ObSSMetaService::create_tablet_), but the local tablet stays at
  // reorgan_scn1 (min_scn), so a create_tablet(reorgan_scn2) call returns
  // OB_ERR_UNEXPECTED. Instead, clone the base version's full serialized value
  // and insert a new row keyed by reorgan_scn2 directly via insert_tablet_meta,
  // which does not validate the local tablet and still yields a row that
  // get_tablet_iter can deserialize into a valid tablet.
  const share::SCN reorgan_scn1 = share::SCN::min_scn();
  const share::SCN reorgan_scn2 = share::SCN::plus(share::SCN::base_scn(), 100);
  ASSERT_NE(reorgan_scn1, reorgan_scn2);
  ObSSMetaTabletKey inserted_reorgan2_key;  // tracked for cleanup at end of case
  {
    ObArenaAllocator base_alloc("BaseTabVal");
    ObTabletHandle base_hdl;
    share::SCN base_meta_scn;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(ls_id, tablet_id, reorgan_scn1,
                                                base_alloc, base_hdl, base_meta_scn));
    ASSERT_TRUE(base_hdl.is_valid());
    ASSERT_TRUE(base_meta_scn.is_valid());

    ObSSMetaTabletKey base_key;
    base_key.ls_id_ = ls_id;
    base_key.tablet_id_ = tablet_id;
    base_key.reorganization_scn_ = reorgan_scn1;
    base_key.meta_scn_ = base_meta_scn;
    ObSSMetaTabletRow base_row;
    ASSERT_EQ(OB_SUCCESS, meta_svr->read_tablet_meta(base_key, base_alloc, base_row));
    ASSERT_GT(base_row.value_.length(), 0);

    inserted_reorgan2_key.ls_id_ = ls_id;
    inserted_reorgan2_key.tablet_id_ = tablet_id;
    inserted_reorgan2_key.reorganization_scn_ = reorgan_scn2;
    inserted_reorgan2_key.meta_scn_ = share::SCN::plus(base_meta_scn, 1);
    ASSERT_TRUE(inserted_reorgan2_key.is_valid());

    // base_row.value_ lives in base_alloc; insert_tablet_meta persists (copies)
    // the row into __all_shared_meta, so it is safe to release base_alloc once
    // the call returns.
    ObSSMetaTabletRow reorgan2_row(
        static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META),
        inserted_reorgan2_key,
        base_row.value_);
    ASSERT_EQ(OB_SUCCESS, meta_svr->insert_tablet_meta(reorgan2_row));
    LOG_INFO("insert second reorgan_scn version succeed", K(ls_id), K(tablet_id),
             K(reorgan_scn2), K(inserted_reorgan2_key));
  }

  ObArenaAllocator allocator("TestTabletIter2");

  // ---- sub-case 1: valid reorgan_scn -> exactly one version ----
  {
    ObSSMetaReadParamV2 read_param;
    read_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        ls_id,
        tablet_id,
        share::SCN::min_scn(),
        reorgan_scn1 /*reorgan_scn*/,
        true /*is_in_table*/);
    read_param.set_read_mark_delete(true);

    ObSSMetaIterGuard<ObSSMetaTabletIter> iter_guard;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_iter(read_param, share::SCN::max_scn(), iter_guard));

    ObSSMetaTabletIter *iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
    ASSERT_NE(nullptr, iter);

    ObTabletHandle tablet_handle;
    share::SCN reorganization_scn;
    share::SCN meta_version;
    int64_t count = 0;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = iter->get_next(allocator, tablet_handle,
                                               reorganization_scn, meta_version))) {
      ASSERT_TRUE(tablet_handle.is_valid());
      ASSERT_NE(nullptr, tablet_handle.get_obj());
      ASSERT_TRUE(meta_version.is_valid());
      ASSERT_EQ(tablet_id.id(), tablet_handle.get_obj()->get_tablet_meta().tablet_id_.id());
      ASSERT_EQ(reorgan_scn1, reorganization_scn);
      LOG_INFO("get_tablet_iter(valid reorgan_scn): got tablet", K(reorganization_scn), K(meta_version));
      count++;
      tablet_handle.reset();
    }
    ASSERT_EQ(OB_ITER_END, ret);
    // prefix pinned to a single reorganization_scn -> exactly one row
    ASSERT_EQ(1, count);
    LOG_INFO("get_tablet_iter valid reorgan_scn sub-case succeed", K(count));
  }

  // ---- sub-case 2: invalid reorgan_scn -> all reorganization_scn versions ----
  {
    ObSSMetaReadParamV2 read_param;
    read_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        ls_id,
        tablet_id,
        share::SCN::min_scn(),
        share::SCN::invalid_scn() /*reorgan_scn*/,
        true /*is_in_table*/);
    read_param.set_read_mark_delete(true);

    ObSSMetaIterGuard<ObSSMetaTabletIter> iter_guard;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_iter(read_param, share::SCN::max_scn(), iter_guard));

    ObSSMetaTabletIter *iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
    ASSERT_NE(nullptr, iter);

    ObTabletHandle tablet_handle;
    share::SCN reorganization_scn;
    share::SCN meta_version;
    int64_t count = 0;
    bool found_reorgan1 = false;
    bool found_reorgan2 = false;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = iter->get_next(allocator, tablet_handle,
                                               reorganization_scn, meta_version))) {
      ASSERT_TRUE(tablet_handle.is_valid());
      ASSERT_NE(nullptr, tablet_handle.get_obj());
      ASSERT_TRUE(meta_version.is_valid());
      // every returned row still belongs to the requested tablet, only the
      // reorganization_scn differs across rows.
      ASSERT_EQ(tablet_id.id(), tablet_handle.get_obj()->get_tablet_meta().tablet_id_.id());
      if (reorganization_scn == reorgan_scn1) {
        found_reorgan1 = true;
      } else if (reorganization_scn == reorgan_scn2) {
        found_reorgan2 = true;
      }
      LOG_INFO("get_tablet_iter(invalid reorgan_scn): got tablet", K(reorganization_scn), K(meta_version));
      count++;
      tablet_handle.reset();
    }
    ASSERT_EQ(OB_ITER_END, ret);
    // both reorganization_scn versions of the same tablet are iterated
    ASSERT_GE(count, 2);
    ASSERT_TRUE(found_reorgan1);
    ASSERT_TRUE(found_reorgan2);
    LOG_INFO("get_tablet_iter invalid reorgan_scn sub-case succeed", K(count));
  }

  // cleanup the inserted reorgan_scn2 version; the base reorgan_scn1 version is
  // left for other cases / TearDown residual cleaner.
  ASSERT_EQ(OB_SUCCESS, delete_tablet_meta_row(*meta_svr, inserted_reorgan2_key));

  LOG_INFO("[TEST CASE END] get_tablet_iter_tablet_prefix");
}

TEST_F(TestSharedMetaService, mark_and_check_tablet_deleted)
{
  LOG_INFO("[TEST CASE BEGIN] mark_and_check_tablet_deleted");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  // create a second table for the mark_deleted test
  ASSERT_EQ(OB_SUCCESS, exe_sql("create table test_svc2_table2 (a int)"));
  LOG_INFO("create table test_svc2_table2 done");

  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id("test_svc2_table2", tablet_id, ls_id));
  LOG_INFO("got tablet_id and ls_id for table2", K(tablet_id), K(ls_id));

  const share::SCN reorgan_scn = share::SCN::min_scn();
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id, tablet_id, reorgan_scn, true/*check_lease*/));
  LOG_INFO("create_tablet for table2 succeed", K(ls_id), K(tablet_id));

  // check_tablet_mark_deleted before marking
  bool is_mark_deleted = true;
  ASSERT_EQ(OB_SUCCESS, meta_svr->check_tablet_mark_deleted(
      ls_id, tablet_id, reorgan_scn, is_mark_deleted));
  ASSERT_FALSE(is_mark_deleted);
  LOG_INFO("check before mark: not deleted", K(tablet_id));

  // mark_tablet_deleted
  ASSERT_EQ(OB_SUCCESS, meta_svr->mark_tablet_deleted(
      ls_id, tablet_id, reorgan_scn, share::SCN::base_scn()));
  LOG_INFO("mark_tablet_deleted succeed", K(tablet_id));

  // check_tablet_mark_deleted after marking
  is_mark_deleted = false;
  ASSERT_EQ(OB_SUCCESS, meta_svr->check_tablet_mark_deleted(
      ls_id, tablet_id, reorgan_scn, is_mark_deleted));
  ASSERT_TRUE(is_mark_deleted);
  LOG_INFO("check after mark: deleted", K(tablet_id));
  LOG_INFO("[TEST CASE END] mark_and_check_tablet_deleted");
}

// ====================== Category 4: sys tenant tiered-meta ======================

// Build a sys-tenant ObTieredMetadataStore that targets __all_shared_tiered_meta
// (the v2 store sub_meta is actually written into for the sys tenant).
#define INIT_SYS_TIERED_STORE_V2(store)                                                  \
  ObTieredMetadataStore store;                                                          \
  ASSERT_EQ(OB_SUCCESS, store.init(OB_SYS_TENANT_ID, GCTX.sql_proxy_));

// Force the next create_tablet() to take the real persist path (the OB_OBJECT_NOT_EXIST
// branch) instead of the idempotent "already exists" short-circuit: drop any existing
// first-level tablet_meta row and any sub_meta rows for this tablet.
#define CLEAN_SYS_TABLET_META(meta_svr, store, ls_id, tablet_id, reorgan_scn)            \
  do {                                                                                   \
    /* first-level tablet_meta in __all_shared_meta */                                   \
    ObSSMetaDeleteParamV2 del_param;                                                      \
    del_param.set_tablet_level_param(                                                     \
        ObSSMetaReadParamType::TABLET_PREFIX,                                             \
        ObSSMetaReadResultType::READ_WHOLE_ROW,                                          \
        ObAtomicProtocolTypeEnum::TABLET_META,                                            \
        (ls_id), (tablet_id), share::SCN::min_scn(), (reorgan_scn),                       \
        true /*is_in_table*/);                                                            \
    int del_ret = (meta_svr)->delete_meta_row(del_param);                                 \
    ASSERT_TRUE(OB_SUCCESS == del_ret || OB_ENTRY_NOT_EXIST == del_ret);                  \
    /* second-level sub_meta in __all_shared_tiered_meta */                              \
    ObArray<blocksstable::MacroBlockId> stale_ids;                                        \
    ASSERT_EQ(OB_SUCCESS, (store).list_meta_macro_files(                                  \
        (tablet_id).is_ls_inner_tablet(), (ls_id).id(), (tablet_id).id(),                 \
        (reorgan_scn).get_val_for_tx(), stale_ids));                                      \
    if (stale_ids.count() > 0) {                                                          \
      ASSERT_EQ(OB_SUCCESS, (store).batch_del_meta(stale_ids));                           \
    }                                                                                     \
  } while (0)

// Verify PRIMARY publication with PALF qualification: non-writer retries must not disrupt
// initial creation or updates.
TEST_F(TestSharedMetaService, ddl_finish_single_writer_publication)
{
  DDLFinishLeaseGuard lease_guard;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  // Override only this thread's role view; background threads retain the real tenant context.
  share::ObTenantBase *saved_tenant = share::ObTenantEnv::get_tenant();
  share::ObTenantBase test_tenant(saved_tenant->id());
  test_tenant = *saved_tenant;
  test_tenant.set_tenant_role(share::ObTenantRole::PRIMARY_TENANT);
  auto role_guard = MAKE_SCOPE(saved_tenant, share::ObTenantEnv::set_tenant);
  share::ObTenantEnv::set_tenant(&test_tenant);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  INIT_SYS_TIERED_STORE_V2(tiered_store);
  // Hidden DDL tablets start without a Major. Normal CREATE TABLE adds an empty Major
  // and cannot represent this intermediate state.
  const ObTabletID tablet_id(ObTimeUtility::current_time());
  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLSTabletService &tablet_svr = *ls_handle.get_ls()->get_tablet_svr();
  ObArenaAllocator allocator("TestDDLWriter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  uint64_t data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), data_version));
  share::schema::ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  table_schema.set_table_state_flag(share::schema::TABLE_STATE_HIDDEN);
  ObCreateTabletSchema create_schema;
  const lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
  ASSERT_EQ(OB_SUCCESS, create_schema.init(allocator, table_schema, compat_mode,
      false/*skip_column_info*/, data_version));
  ObSEArray<ObTabletID, 1> tablet_ids;
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id));
  ObTabletHandle local_tablet;
  ASSERT_EQ(OB_SUCCESS, tablet_svr.create_tablet(ls_id, tablet_id, tablet_id, SCN::base_scn(),
      1/*snapshot_version*/, create_schema, compat_mode, false/*need_create_empty_major_sstable*/,
      SCN::invalid_scn(), false/*micro_index_clustered*/, false/*has_cs_replica*/,
      ObSplitTabletInfo(), ObForkTabletInfo(), data_version, local_tablet));
  DDLFinishTabletGuard tablet_guard(tablet_svr, tablet_ids);
  ASSERT_FALSE(local_tablet.get_obj()->get_tablet_meta().table_store_flag_.with_major_sstable());
  ASSERT_EQ(0, local_tablet.get_obj()->get_major_table_count());
  const SCN reorgan_scn = local_tablet.get_obj()->get_reorganization_scn();
  CLEAN_SYS_TABLET_META(meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn);

  ObStorageSchema *loaded_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, local_tablet.get_obj()->load_storage_schema(allocator, loaded_schema));
  ObStorageSchema storage_schema;
  const int schema_ret = storage_schema.init(allocator, *loaded_schema);
  ObTabletObjLoadHelper::free(allocator, loaded_schema);
  ASSERT_EQ(OB_SUCCESS, schema_ret);
  const int64_t snapshot_version = ObTimeUtility::current_time_ns();
  ObTabletCreateSSTableParam create_param;
  ASSERT_EQ(OB_SUCCESS, create_param.init_for_empty_major_sstable(tablet_id, storage_schema,
      snapshot_version, -1/*schema_version*/, false/*is_tmp_sstable*/, true/*is_shared_storage*/));
  ObTableHandleV2 major_handle;
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable(
      create_param, allocator, major_handle));
  blocksstable::ObSSTable *major = nullptr;
  ASSERT_EQ(OB_SUCCESS, major_handle.get_sstable(major));
  ASSERT_NE(nullptr, major);
  const int64_t buf_len = major->get_serialize_size(data_version);
  char *buf = static_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, major->serialize(data_version, buf, buf_len, pos));
  const ObString sstable_buf(pos, buf);
  ObArray<ObSSMetaCtrlKeyInfo> empty_keys;

  ObTabletAtomicProtocol creator;
  ObTabletAtomicProtocolParam creator_param;
  ASSERT_EQ(OB_SUCCESS, meta_svr->build_tablet_protocol_init_param_(
      ls_id, tablet_id, reorgan_scn, empty_keys, creator_param));
  ASSERT_EQ(OB_SUCCESS, creator.init(creator_param));
  ASSERT_EQ(OB_SUCCESS, creator.create(true /*need_check_sswriter*/));
  int64_t creator_op_id = -1;
  ASSERT_EQ(OB_SUCCESS, creator.get_op_id(creator_op_id));
  ObSSMetaAtomicOpInitParam op_param;
  meta_svr->build_tablet_op_init_param_(creator_op_id, ls_id, tablet_id, reorgan_scn, op_param);
  ObSSMetaAtomicTabletOp tablet_op;
  ASSERT_EQ(OB_SUCCESS, tablet_op.init(op_param));
  ObSSMetaCtrlKey tablet_key;
  tablet_key.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  tablet_key.tablet_key_.tablet_id_ = tablet_id;
  tablet_key.tablet_key_.reorg_scn_ = reorgan_scn;
  tablet_key.tablet_key_.control_scn_ = SCN::min_scn();
  ObSSMetaCtrlKey lock_key;
  ASSERT_EQ(OB_SUCCESS, tablet_key.get_lock_row_key(lock_key));
  ObLockAtomicProtocol lock_row;
  ASSERT_EQ(OB_SUCCESS, lock_row.init(MTL_ID(), ls_id, lock_key));
  ObSSMetaCtrlLockRowValue lock_before;
  ASSERT_EQ(OB_SUCCESS, lock_row.read_lock_row(*GCTX.sql_proxy_, lock_before));
  ASSERT_EQ(creator_op_id + 1, lock_before.op_id_);

  // Pause between creator start and finish; simulate four replay retries for each
  // of two non-writer replicas.
  mock_is_sswriter = false;
  for (int64_t replica = 0; replica < 2; ++replica) {
    for (int64_t retry = 0; retry < 4; ++retry) {
      check_lease_call_count = 0;
      ASSERT_EQ(OB_NOT_MASTER, share::ObSSDDLUtil::update_shared_tablet_table_store_if_absent(
          ls_id, local_tablet, major->get_key(), data_version, sstable_buf, true));
      ASSERT_EQ(1, check_lease_call_count);
      ASSERT_EQ(ObSSWriterType::META, last_lease_type);
      ObSSMetaCtrlLockRowValue lock_after;
      ASSERT_EQ(OB_SUCCESS, lock_row.read_lock_row(*GCTX.sql_proxy_, lock_after));
      ASSERT_EQ(lock_before.op_id_, lock_after.op_id_);
    }
  }
  mock_is_sswriter = true;

  // Publish real tablet metadata to verify updates on an absent tablet do not disrupt
  // the creator, rather than only checking a no-op finish.
  ObTabletHandle first_tablet;
  ASSERT_EQ(OB_SUCCESS, meta_svr->acquire_first_created_ss_tablet_(
      allocator, local_tablet, first_tablet));
  ASSERT_EQ(OB_SUCCESS, meta_svr->persist_tablet_(
      data_version, first_tablet.get_obj(), ObMetaUpdateReason::CREATE_TABLET, &tablet_op));
  ASSERT_EQ(OB_SUCCESS, creator.finish(tablet_op));
  ObTabletHandle published_tablet;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(
      ls_id, tablet_id, reorgan_scn, allocator, published_tablet));
  ASSERT_TRUE(published_tablet.is_valid());
  ASSERT_FALSE(
      published_tablet.get_obj()->get_tablet_meta().table_store_flag_.with_major_sstable());

  // Non-writers must not start updates when the shared tablet exists but lacks the target Major.
  mock_is_sswriter = false;
  for (int64_t replica = 0; replica < 2; ++replica) {
    ASSERT_EQ(OB_NOT_MASTER, share::ObSSDDLUtil::update_shared_tablet_table_store_if_absent(
        ls_id, local_tablet, major->get_key(), data_version, sstable_buf, true));
  }
  ObSSMetaCtrlLockRowValue lock_after;
  ASSERT_EQ(OB_SUCCESS, lock_row.read_lock_row(*GCTX.sql_proxy_, lock_after));
  ASSERT_EQ(lock_before.op_id_, lock_after.op_id_);

  // Propagate writer qualification errors unchanged; they imply neither valid qualification
  // nor completed publication.
  mock_is_sswriter = true;
  ddl_lease_error = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT, share::ObSSDDLUtil::update_shared_tablet_table_store_if_absent(
      ls_id, local_tablet, major->get_key(), data_version, sstable_buf, true));
  ddl_lease_error = OB_SUCCESS;

  // Losing qualification after the outer check but before start must be rejected
  // without advancing op_id.
  check_lease_call_count = 0;
  ddl_lease_reject_at = 2;
  ASSERT_EQ(OB_NOT_MASTER, share::ObSSDDLUtil::update_shared_tablet_table_store_if_absent(
      ls_id, local_tablet, major->get_key(), data_version, sstable_buf, true));
  ASSERT_EQ(OB_SUCCESS, lock_row.read_lock_row(*GCTX.sql_proxy_, lock_after));
  ASSERT_EQ(lock_before.op_id_, lock_after.op_id_);

  // Losing qualification between start and finish may advance op_id but must not publish a Major.
  check_lease_call_count = 0;
  ddl_lease_reject_at = 3;
  ASSERT_EQ(OB_NOT_MASTER, share::ObSSDDLUtil::update_shared_tablet_table_store_if_absent(
      ls_id, local_tablet, major->get_key(), data_version, sstable_buf, true));
  ddl_lease_reject_at = 0;
  bool is_major_exist = false;
  ASSERT_EQ(OB_SUCCESS, share::ObSSDDLUtil::check_ddl_shared_major_exist(
      ls_id, tablet_id, reorgan_scn, snapshot_version, allocator, is_major_exist));
  ASSERT_FALSE(is_major_exist);

  // PRIMARY finish must not publish after proposal_id changes, even if writer qualification holds.
  check_lease_call_count = 0;
  ddl_epoch_change_at = 3;
  ASSERT_EQ(OB_NOT_MASTER, share::ObSSDDLUtil::update_shared_tablet_table_store_if_absent(
      ls_id, local_tablet, major->get_key(), data_version, sstable_buf, true));
  ddl_epoch_change_at = 0;
  ASSERT_EQ(OB_SUCCESS, share::ObSSDDLUtil::check_ddl_shared_major_exist(
      ls_id, tablet_id, reorgan_scn, snapshot_version, allocator, is_major_exist));
  ASSERT_FALSE(is_major_exist);

  // Once a qualified writer publishes, non-writers need neither payload deserialization
  // nor another qualification check.
  ASSERT_EQ(OB_SUCCESS, share::ObSSDDLUtil::update_shared_tablet_table_store_if_absent(
      ls_id, local_tablet, major->get_key(), data_version, sstable_buf, true));
  ObTabletHandle updated_tablet;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(
      ls_id, tablet_id, reorgan_scn, allocator, updated_tablet));
  ASSERT_TRUE(updated_tablet.get_obj()->get_tablet_meta().table_store_flag_.with_major_sstable());
  ASSERT_EQ(snapshot_version, updated_tablet.get_obj()->get_last_compaction_scn());
  ASSERT_EQ(OB_SUCCESS, lock_row.read_lock_row(*GCTX.sql_proxy_, lock_before));
  mock_is_sswriter = false;
  ddl_lease_error = OB_TIMEOUT;
  check_lease_call_count = 0;
  ASSERT_EQ(OB_SUCCESS, share::ObSSDDLUtil::update_shared_tablet_table_store_if_absent(
      ls_id, local_tablet, major->get_key(), data_version, ObString(), true));
  ASSERT_EQ(0, check_lease_call_count);
  ASSERT_EQ(OB_SUCCESS, lock_row.read_lock_row(*GCTX.sql_proxy_, lock_after));
  ASSERT_EQ(lock_before.op_id_, lock_after.op_id_);
  ddl_lease_error = OB_SUCCESS;
  mock_is_sswriter = true;

  updated_tablet.reset();
  published_tablet.reset();
  first_tablet.reset();

  // A writer can publish an absent tablet through update/create/update;
  // retries must preserve the parameters.
  CLEAN_SYS_TABLET_META(meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn);
  check_lease_call_count = 0;
  ASSERT_EQ(OB_SUCCESS, share::ObSSDDLUtil::update_shared_tablet_table_store_if_absent(
      ls_id, local_tablet, major->get_key(), data_version, sstable_buf, true));
  ASSERT_GE(check_lease_call_count, 6);

  // Normal producer paths explicitly disable the new qualification check,
  // preserving the update contract for an existing tablet.
  CLEAN_SYS_TABLET_META(meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn);
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id, tablet_id, reorgan_scn, true));
  mock_is_sswriter = false;
  check_lease_call_count = 0;
  ASSERT_EQ(OB_SUCCESS, share::ObSSDDLUtil::update_shared_tablet_table_store_if_absent(
      ls_id, local_tablet, major->get_key(), data_version, sstable_buf, false/*check_lease*/));
  ASSERT_EQ(0, check_lease_call_count);
  mock_is_sswriter = true;
  major_handle.reset();
  local_tablet.reset();
  CLEAN_SYS_TABLET_META(meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn);
}

// Standby/restore replay must wait for a lease even when this thread has PALF qualification.
TEST_F(TestSharedMetaService, ddl_finish_waits_for_restore_writer)
{
  DDLFinishLeaseGuard lease_guard;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObTabletID source_tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id("test_svc2_table", source_tablet_id, ls_id));
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLSTabletService &tablet_svr = *ls_handle.get_ls()->get_tablet_svr();
  ObArenaAllocator allocator("TestDDLWriter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  uint64_t data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), data_version));
  // Use an isolated hidden tablet: earlier cases may publish the source table's empty Major.
  const ObTabletID tablet_id(ObTimeUtility::current_time());
  share::schema::ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  table_schema.set_tenant_id(MTL_ID());
  table_schema.set_table_state_flag(share::schema::TABLE_STATE_HIDDEN);
  ObCreateTabletSchema create_schema;
  const lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
  ASSERT_EQ(OB_SUCCESS, create_schema.init(allocator, table_schema, compat_mode,
      false/*skip_column_info*/, data_version));
  ObSEArray<ObTabletID, 1> tablet_ids;
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id));
  // Keep background writers from publishing the tablet while preparing the waiting state.
  ATOMIC_STORE(&mock_sswriter_enabled, false);
  mock_replay_writer = true;
  ObTabletHandle local_tablet;
  ASSERT_EQ(OB_SUCCESS, tablet_svr.create_tablet(ls_id, tablet_id, tablet_id, SCN::base_scn(),
      1/*snapshot_version*/, create_schema, compat_mode, false/*need_create_empty_major_sstable*/,
      SCN::invalid_scn(), false/*micro_index_clustered*/, false/*has_cs_replica*/,
      ObSplitTabletInfo(), ObForkTabletInfo(), data_version, local_tablet));
  DDLFinishTabletGuard tablet_guard(tablet_svr, tablet_ids);
  ASSERT_FALSE(local_tablet.get_obj()->get_tablet_meta().table_store_flag_.with_major_sstable());
  ASSERT_EQ(0, local_tablet.get_obj()->get_major_table_count());
  ObITable::TableKey table_key;
  table_key.table_type_ = ObITable::MAJOR_SSTABLE;
  table_key.tablet_id_ = tablet_id;
  table_key.version_range_.snapshot_version_ = ObTimeUtility::current_time_ns();
  bool is_major_exist = false;
  ASSERT_EQ(OB_SUCCESS, share::ObSSDDLUtil::check_ddl_shared_major_exist(
      ls_id, tablet_id, local_tablet.get_obj()->get_reorganization_scn(),
      table_key.get_snapshot_version(), allocator, is_major_exist));
  ASSERT_FALSE(is_major_exist);

  // Switch only this thread's role; an empty payload verifies rejection before deserialization.
  share::ObTenantBase *saved_tenant = share::ObTenantEnv::get_tenant();
  share::ObTenantBase test_tenant(saved_tenant->id());
  test_tenant = *saved_tenant;
  auto role_guard = MAKE_SCOPE(saved_tenant, share::ObTenantEnv::set_tenant);
  share::ObTenantEnv::set_tenant(&test_tenant);
  for (const auto role : {share::ObTenantRole::STANDBY_TENANT,
      share::ObTenantRole::RESTORE_TENANT}) {
    test_tenant.set_tenant_role(role);
    check_lease_call_count = 0;
    ASSERT_EQ(OB_NOT_MASTER, share::ObSSDDLUtil::update_shared_tablet_table_store_if_absent(
        ls_id, local_tablet, table_key, data_version, ObString(), true));
    ASSERT_EQ(1, check_lease_call_count);
    ASSERT_EQ(ObSSWriterType::META, last_lease_type);
  }
}

// Requirement 1: a sys-tenant tablet's tablet_sub_meta must be persisted into the
// internal tiered metadata table (__all_shared_tiered_meta), regardless of its size.
TEST_F(TestSharedMetaService, sys_tablet_sub_meta_in_tiered_table)
{
  LOG_INFO("[TEST CASE BEGIN] sys_tablet_sub_meta_in_tiered_table");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  INIT_SYS_TIERED_STORE_V2(tiered_store);

  // create a sys-tenant user table and locate its tablet
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("create database if not exists ss_meta_test"));
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("create table if not exists ss_meta_test.sys_sub_meta_tbl (a int)"));

  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_sys_tablet_and_ls_id("sys_sub_meta_tbl", tablet_id, ls_id));
  ASSERT_TRUE(tablet_id.is_valid());
  ASSERT_TRUE(ls_id.is_valid());
  LOG_INFO("got sys tablet_id and ls_id", K(tablet_id), K(ls_id));

  // make sure create_tablet really persists (avoid the idempotent already-exists path).
  const share::SCN reorgan_scn = share::SCN::min_scn();
  CLEAN_SYS_TABLET_META(meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn);

  // This drives the sys-tenant persist path that writes tablet_sub_meta into the
  // internal tiered metadata table.
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id, tablet_id, reorgan_scn, true/*check_lease*/));

  // tablet_sub_meta should now be present in the tiered metadata table. A non
  // empty-shell tablet persists at least the table_store and storage_schema
  // sub_meta entries, so the count is >= 1 (typically 2).
  const int64_t reorg_val = reorgan_scn.get_val_for_tx();
  ObArray<blocksstable::MacroBlockId> in_table_ids;
  ASSERT_EQ(OB_SUCCESS, tiered_store.list_meta_macro_files(
      tablet_id.is_ls_inner_tablet(), ls_id.id(), tablet_id.id(), reorg_val, in_table_ids));
  ASSERT_GE(in_table_ids.count(), 1);
  for (int64_t i = 0; i < in_table_ids.count(); ++i) {
    ASSERT_TRUE(in_table_ids.at(i).is_shared_tablet_sub_meta_in_table());
    bool exist = false;
    ASSERT_EQ(OB_SUCCESS, tiered_store.is_meta_exist(in_table_ids.at(i), exist));
    ASSERT_TRUE(exist);
  }
  LOG_INFO("sys tablet_sub_meta found in tiered table", K(tablet_id), K(ls_id),
           "sub_meta_cnt", in_table_ids.count());

  LOG_INFO("[TEST CASE END] sys_tablet_sub_meta_in_tiered_table");
}

/**
 * 概要：dump iterator 必须能读出超过旧 4 MiB 上限的 Tiered storage schema。
 *
 * 覆盖模块：
 * 1. ObSSMetaService / ObSSTabletPersister：
 *    创建并持久化真实 SYS tablet，生成 iterator 所需的根 meta 和 sub-meta 引用。
 * 2. ObTieredMetadataStore：
 *    用同一 schema macro id 保存合法的大对象，模拟 tablet schema 已经落入 Tiered 表。
 * 3. ObSSTabletMetaInfoIterator：
 *    走公开 init/get_next，串起根 meta 解析、Tiered schema 读取和 level=2 行输出。
 * 4. SysLsCkptPauseGuard：
 *    隔离后台 checkpoint，避免它与 schema 行替换、根 meta 发布和清理并发竞争。
 *
 * 测试流程：
 * 1. 创建唯一 SYS 表，清理幂等残留后重新持久化 tablet，确保不是已存在旁路。
 * 2. 从真实 tablet 取得精确 tablet_meta_scn 和 storage schema addr。
 * 3. 构造合法的 4 MiB+ JSON 属性，初始化 StorageSchema，并做序列化往返校验。
 * 4. 暂停 SYS checkpoint，新写一版 addr.size 与大 schema 一致的根 meta，再用相同 macro id 替换 schema 行。
 * 5. 走 iterator 的 init/get_next，确认输出 level=2 的 storage schema 行。
 * 6. 释放 tablet handle，清理 SQL 表、shared meta 行和 Tiered sub-meta 行。
 *
 * 回归判据：旧定长读法会吞掉大 schema 的读取失败，iterator 最终缺少 schema 行。
 */
TEST_F(TestSharedMetaService, dump_iterator_reads_large_tiered_schema)
{
  LOG_INFO("[TEST CASE BEGIN] dump_iterator_reads_large_tiered_schema");
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  INIT_SYS_TIERED_STORE_V2(tiered_store);

  // 每次使用唯一表名，避免上次异常退出留下的 DDL 状态影响 create_tablet 路径。
  const int64_t table_suffix = ObTimeUtility::current_time();
  ObSqlString table_name;
  ObSqlString create_table_sql;
  ObSqlString drop_table_sql;
  ASSERT_EQ(OB_SUCCESS,
            sys_exe_sql("create database if not exists ss_meta_test"));
  ASSERT_EQ(OB_SUCCESS,
            table_name.assign_fmt(
                "dump_large_schema_%ld", table_suffix));
  ASSERT_EQ(OB_SUCCESS,
            create_table_sql.assign_fmt(
                "create table ss_meta_test.%s (a int)", table_name.ptr()));
  ASSERT_EQ(OB_SUCCESS,
            drop_table_sql.assign_fmt(
                "drop table if exists ss_meta_test.%s", table_name.ptr()));

  const share::SCN reorgan_scn = share::SCN::min_scn();
  SysLsCkptPauseGuard ckpt_pause_guard;
  ObTabletID tablet_id;
  ObLSID ls_id;
  bool ckpt_pause_attempted = false;
  // 清理可能与后台 checkpoint 竞争，先暂停 checkpoint，再删除 SQL 和 shared meta 数据。
  auto cleanup_resources = [&]() {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    if (!ckpt_pause_attempted) {
      ckpt_pause_attempted = true;
      tmp_ret = ckpt_pause_guard.pause();
      if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("pause checkpoint before iterator cleanup failed",
                 KR(tmp_ret), K(tablet_id), K(ls_id));
      }
    }
    tmp_ret = sys_exe_sql(drop_table_sql.ptr());
    if (OB_SUCCESS != tmp_ret) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
      LOG_WARN("drop iterator test table failed",
               KR(tmp_ret), K(tablet_id), K(ls_id));
    }
    if (tablet_id.is_valid() && ls_id.is_valid()) {
      tmp_ret = cleanup_sys_tablet_meta(
          *meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn);
      if (OB_SUCCESS != tmp_ret) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        LOG_WARN("cleanup iterator test tablet meta failed",
                 KR(tmp_ret), K(tablet_id), K(ls_id));
      }
    }
    return ret;
  };
  bool resources_need_cleanup = true;
  auto resources_cleanup_guard = MAKE_SCOPE(
      resources_need_cleanup, [&](bool &needs_cleanup) {
        if (needs_cleanup) {
          const int tmp_ret = cleanup_resources();
          if (OB_SUCCESS != tmp_ret) {
            ADD_FAILURE() << "清理 dump iterator 测试资源失败, ret="
                          << tmp_ret;
          }
        }
      });

  // 清掉 create table 产生的初始持久化结果，再显式走一次真实的 tablet persist 路径。
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(create_table_sql.ptr()));
  ASSERT_EQ(OB_SUCCESS,
            get_sys_tablet_and_ls_id(table_name.ptr(), tablet_id, ls_id));
  ASSERT_TRUE(tablet_id.is_valid());
  ASSERT_TRUE(ls_id.is_sys_ls());
  ASSERT_EQ(OB_SUCCESS,
            cleanup_sys_tablet_meta(
                *meta_svr,
                tiered_store,
                ls_id,
                tablet_id,
                reorgan_scn));
  ASSERT_EQ(OB_SUCCESS,
            meta_svr->create_tablet(
                ls_id,
                tablet_id,
                reorgan_scn,
                true /*check_lease*/));

  ObArenaAllocator allocator("LargeSchemaIter");
  ObTabletHandle tablet_handle;
  share::SCN tablet_meta_scn;
  ASSERT_EQ(OB_SUCCESS,
            meta_svr->get_tablet(
                ls_id,
                tablet_id,
                reorgan_scn,
                allocator,
                tablet_handle,
                tablet_meta_scn));
  ASSERT_TRUE(tablet_handle.is_valid());
  ASSERT_TRUE(tablet_meta_scn.is_valid());

  // iterator 必须使用根 meta 中的精确 SCN 和 schema 地址，不能手工拼一个旁路 key。
  const ObMetaDiskAddr original_schema_addr =
      tablet_handle.get_obj()->get_storage_schema_addr();
  blocksstable::MacroBlockId schema_macro_id;
  ASSERT_EQ(OB_SUCCESS,
            original_schema_addr.get_macro_block_id(schema_macro_id));
  ASSERT_TRUE(schema_macro_id.is_shared_tablet_sub_meta_in_table());
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());
  ckpt_pause_attempted = true;

  // JSON 中的尾随空白合法；借此放大属性而不依赖大量易变的列定义。
  share::schema::ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  share::schema::ObSemiStructEncodingType encoding_type;
  encoding_type.mode_ = share::ObSemistructProperties::ENCODING;
  table_schema.set_semistruct_encoding_type(encoding_type);

  const int64_t property_size = 4 * 1024 * 1024 + 1;
  char *property_buf = static_cast<char *>(allocator.alloc(property_size));
  ASSERT_NE(nullptr, property_buf);
  MEMSET(property_buf, ' ', property_size);
  const char *property_prefix = "{\"encoding_type\":1";
  const int64_t property_prefix_len = STRLEN(property_prefix);
  ASSERT_LT(property_prefix_len, property_size);
  MEMCPY(property_buf, property_prefix, property_prefix_len);
  property_buf[property_size - 1] = '}';
  const ObString semistruct_properties(
      static_cast<int32_t>(property_size), property_buf);
  ASSERT_EQ(OB_SUCCESS,
            table_schema.set_semistruct_properties(semistruct_properties));

  // 先完成 StorageSchema 的序列化往返，证明注入的是可被生产代码解析的合法对象。
  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS,
            storage_schema.init(
                allocator,
                table_schema,
                lib::Worker::CompatMode::MYSQL));
  ASSERT_TRUE(storage_schema.is_valid());
  const int64_t schema_size = storage_schema.get_serialize_size();
  ASSERT_GT(schema_size, 4 * 1024 * 1024);
  char *schema_buf = static_cast<char *>(allocator.alloc(schema_size));
  ASSERT_NE(nullptr, schema_buf);
  int64_t schema_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            storage_schema.serialize(schema_buf, schema_size, schema_pos));
  ASSERT_EQ(schema_size, schema_pos);

  ObStorageSchema decoded_schema;
  int64_t decoded_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            decoded_schema.deserialize(
                allocator,
                schema_buf,
                schema_size,
                decoded_pos));
  ASSERT_EQ(schema_size, decoded_pos);
  ASSERT_TRUE(decoded_schema.is_valid());
  ASSERT_EQ(property_size,
            decoded_schema.get_semistruct_properties().length());

  // 生产持久化会把 sub-meta 的精确长度写入 ObMetaDiskAddr。测试替换 schema value 时也同步
  // 生成一版长度一致的根 meta，避免构造“addr.size 小于实际 value”的非生产状态。
  ObSSMetaTabletKey source_tablet_key;
  source_tablet_key.ls_id_ = ls_id;
  source_tablet_key.tablet_id_ = tablet_id;
  source_tablet_key.reorganization_scn_ = reorgan_scn;
  source_tablet_key.meta_scn_ = tablet_meta_scn;
  ObSSMetaTabletRow source_tablet_row;
  ASSERT_EQ(OB_SUCCESS,
            meta_svr->read_tablet_meta(
                source_tablet_key, allocator, source_tablet_row));

  ObSSLsCkptWriter meta_builder(true /*need_check_existing_object*/);
  meta_builder.init_write_tablet_context_(
      source_tablet_key, tiered_store, allocator);
  ASSERT_EQ(OB_SUCCESS,
            meta_builder.build_tablet_meta_(source_tablet_row.value_));
  ASSERT_NE(nullptr, meta_builder.tablet_);
  ObMetaDiskAddr &rewritten_schema_addr =
      meta_builder.tablet_->storage_schema_addr_.addr_;
  blocksstable::MacroBlockId rewritten_schema_macro_id;
  int64_t rewritten_schema_offset = 0;
  int64_t original_schema_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            rewritten_schema_addr.get_block_addr(
                rewritten_schema_macro_id,
                rewritten_schema_offset,
                original_schema_size));
  ASSERT_EQ(schema_macro_id, rewritten_schema_macro_id);
  ASSERT_EQ(0, rewritten_schema_offset);
  ASSERT_GT(original_schema_size, 0);
  ASSERT_EQ(OB_SUCCESS,
            rewritten_schema_addr.set_block_addr(
                rewritten_schema_macro_id,
                rewritten_schema_offset,
                schema_size,
                rewritten_schema_addr.type()));
  ASSERT_EQ(schema_size,
            static_cast<int64_t>(rewritten_schema_addr.size()));

  ObString rewritten_tablet_value;
  ASSERT_EQ(OB_SUCCESS,
            meta_builder.serialize_tablet_meta_(rewritten_tablet_value));
  ObSSMetaTabletKey large_schema_tablet_key(source_tablet_key);
  large_schema_tablet_key.meta_scn_ =
      share::SCN::plus(tablet_meta_scn, 1);
  ASSERT_TRUE(large_schema_tablet_key.is_valid());
  const ObSSMetaTabletRow large_schema_tablet_row(
      static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META),
      large_schema_tablet_key,
      rewritten_tablet_value);
  ASSERT_EQ(OB_SUCCESS,
            meta_svr->insert_tablet_meta(large_schema_tablet_row));

  // 保持 macro id 不变，只替换它引用的 schema value 为 4 MiB+ 数据。
  ASSERT_EQ(OB_SUCCESS, tiered_store.del_meta(schema_macro_id));
  ASSERT_EQ(OB_SUCCESS,
            tiered_store.write_meta(
                schema_macro_id,
                static_cast<int64_t>(ObSSTieredMetaTypeEnum::STORAGE_SCHEMA),
                ObString(static_cast<int32_t>(schema_size), schema_buf)));

  // 从公开入口遍历所有层级；Tiered schema 读取成功后才会产生 level=2 的 storage schema 行。
  ObSSTabletMetaInfoIterator iter;
  ASSERT_EQ(OB_SUCCESS,
            iter.init(
                *GCTX.sql_proxy_,
                OB_SYS_TENANT_ID,
                ls_id,
                tablet_id,
                reorgan_scn,
                large_schema_tablet_key.meta_scn_,
                obrpc::ObDumpSSTabletMetaInfoMode::SIMPLE));
  bool found_schema_row = false;
  oceanbase::storage::ObSSTabletMetaInfoRow row;
  int iter_ret = OB_SUCCESS;
  while (OB_SUCCESS == (iter_ret = iter.get_next(row))) {
    if (2 == row.level_
        && 0 == row.type_.case_compare("storage schema")) {
      found_schema_row = true;
      ASSERT_FALSE(row.meta_value_.empty());
    }
  }
  ASSERT_EQ(OB_ITER_END, iter_ret);
  ASSERT_TRUE(found_schema_row);

  // 主流程显式清理并关闭 scope guard，避免析构阶段重复删除同一批资源。
  tablet_handle.reset();
  ASSERT_EQ(OB_SUCCESS, cleanup_resources());
  resources_need_cleanup = false;
  LOG_INFO("[TEST CASE END] dump_iterator_reads_large_tiered_schema",
           K(tablet_id), K(ls_id), K(schema_size), K(large_schema_tablet_key));
}

/**
 * 概要：checkpoint 上传时，只带“当前 tablet 真正用到的文件”，
 * 目录里多出来的旧文件不要一起带走。
 *
 * 覆盖模块：
 * 1. ObSSLsCkptWriter：只上传指定 tablet 版本真正引用的 sub-meta，
 *    并把 tablet/table-store 里的表内地址转换为对象存储地址。
 * 2. ObSSLsCkptReader：直接读取已经转换好的 checkpoint tablet meta，
 *    不再在恢复阶段修改地址，同时生成后续更新需要的 update log。
 *
 * 测试流程（用一个“目录里有旧文件”的场景验证版本过滤）：
 * 1. 在 SYS tenant 创建一张测试表，拿到它的 tablet_id、LS 和当前 meta_scn。
 * 2. 额外写入一个旧 sub-meta。目录扫描能看到它，但当前 tablet 版本没有引用它。
 * 3. 从当前 tablet 的 macro info 记下实际引用，确认目录扫描能看到旧对象，
 *    但当前 tablet 并没有引用它。
 * 4. 只调用一次 upload_tablet_meta_；它会先解析 tablet、上传真正引用的
 *    sub-meta，再上传 tablet meta。确认旧对象没有被误上传。
 * 5. 用 Reader 恢复 tablet，确认身份、update log 以及 table store 地址已经
 *    直接指向 OSS。
 *
 * 同时覆盖一个旁路：把 table store 行的 meta_type 改成无效值，
 * 上传仍应按 tablet 真实引用识别，而不是依赖目录里的类型字段。
 */
TEST_F(TestSharedMetaService, sys_ls_ckpt_uploads_only_referenced_sub_meta)
{
  LOG_INFO("[TEST CASE BEGIN] sys_ls_ckpt_uploads_only_referenced_sub_meta");
  // 这个用例只验证 SYS tenant 的 shared meta/checkpoint 路径。
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  INIT_SYS_TIERED_STORE_V2(tiered_store);

  const int64_t table_suffix = ObTimeUtility::current_time();
  ObSqlString table_name;
  ObSqlString create_table_sql;
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("create database if not exists ss_meta_test"));
  ASSERT_EQ(OB_SUCCESS, table_name.assign_fmt("sys_ls_ckpt_version_%ld", table_suffix));
  ASSERT_EQ(OB_SUCCESS, create_table_sql.assign_fmt(
      "create table ss_meta_test.%s (a int)", table_name.ptr()));
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(create_table_sql.ptr()));

  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_sys_tablet_and_ls_id(table_name.ptr(), tablet_id, ls_id));
  ASSERT_TRUE(tablet_id.is_valid());
  ASSERT_TRUE(ls_id.is_valid());

  const share::SCN reorgan_scn = share::SCN::min_scn();
  // 先清掉历史残留，保证下面拿到的是本次测试刚写入的 tablet 版本。
  CLEAN_SYS_TABLET_META(meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn);
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(
      ls_id, tablet_id, reorgan_scn, true/*check_lease*/));

  ObArenaAllocator tablet_allocator("CkptVersion");
  ObTabletHandle tablet_handle;
  share::SCN tablet_meta_scn;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(
      ls_id, tablet_id, reorgan_scn, tablet_allocator, tablet_handle, tablet_meta_scn));
  ASSERT_TRUE(tablet_handle.is_valid());
  ASSERT_TRUE(tablet_meta_scn.is_valid());
  ASSERT_TRUE(tablet_handle.get_obj()->is_row_store());

  blocksstable::MacroBlockId table_store_macro_id;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_table_store_addr().get_macro_block_id(
      table_store_macro_id));
  ASSERT_TRUE(table_store_macro_id.is_shared_tablet_sub_meta_in_table());

  ObSSMetaTabletKey tablet_key;
  tablet_key.ls_id_ = ls_id;
  tablet_key.tablet_id_ = tablet_id;
  tablet_key.reorganization_scn_ = reorgan_scn;
  tablet_key.meta_scn_ = tablet_meta_scn;

  ObSSLsCkptWriter ckpt_writer(true /*need_check_existing_object*/);
  ObArray<blocksstable::MacroBlockId> referenced_ids;
  ObTabletMacroInfo *tablet_macro_info = nullptr;
  // 期望集合直接来自当前 tablet；后面仍走 upload_tablet_meta_ 的真实上传链路。
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->load_macro_info(
      0/*ls_epoch*/, tablet_allocator, tablet_macro_info));
  ASSERT_NE(nullptr, tablet_macro_info);
  for (int64_t i = 0; i < tablet_macro_info->shared_meta_block_info_arr_.cnt_; ++i) {
    const blocksstable::MacroBlockId &macro_id =
        tablet_macro_info->shared_meta_block_info_arr_.arr_[i];
    if (macro_id.is_shared_tablet_sub_meta_in_table()) {
      ASSERT_EQ(OB_SUCCESS, referenced_ids.push_back(macro_id));
    }
  }
  ASSERT_GE(referenced_ids.count(), 1);

  // 模拟同一 tablet 目录下遗留的旧版本 sub-meta：目录前缀扫描会返回它，
  // 但当前 tablet meta 并未引用该 op_id。
  int64_t stale_op_id = 0;
  for (int64_t i = 0; i < referenced_ids.count(); ++i) {
    stale_op_id = MAX(stale_op_id, referenced_ids.at(i).op_id() + 1);
  }
  blocksstable::MacroBlockId stale_id = referenced_ids.at(0);
  stale_id.set_op_id(stale_op_id);
  ASSERT_TRUE(stale_id.is_shared_tablet_sub_meta_in_table());
  const ObString stale_value = ObString::make_string("stale tablet sub meta");
  ASSERT_EQ(OB_SUCCESS, tiered_store.write_meta(
      stale_id, static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE), stale_value));
  bool stale_meta_needs_cleanup = true;
  auto stale_meta_guard = MAKE_SCOPE(stale_meta_needs_cleanup, [&](bool &needs_cleanup) {
    if (needs_cleanup) {
      const int tmp_ret = tiered_store.del_meta(stale_id);
      if (OB_SUCCESS != tmp_ret && OB_ENTRY_NOT_EXIST != tmp_ret) {
        ADD_FAILURE() << "清理测试构造的 stale sub-meta 失败, ret=" << tmp_ret;
      }
    }
  });

  ObArray<blocksstable::MacroBlockId> directory_ids;
  ASSERT_EQ(OB_SUCCESS, tiered_store.list_meta_macro_files(
      tablet_id.is_ls_inner_tablet(), ls_id.id(), tablet_id.id(),
      reorgan_scn.get_val_for_inner_table_field(), directory_ids));
  bool stale_found_by_directory_scan = false;
  for (int64_t i = 0; !stale_found_by_directory_scan && i < directory_ids.count(); ++i) {
    stale_found_by_directory_scan = directory_ids.at(i) == stale_id;
  }
  ASSERT_TRUE(stale_found_by_directory_scan);

  bool stale_referenced_by_tablet = false;
  for (int64_t i = 0; !stale_referenced_by_tablet && i < referenced_ids.count(); ++i) {
    stale_referenced_by_tablet = referenced_ids.at(i) == stale_id;
  }
  ASSERT_FALSE(stale_referenced_by_tablet);

  // 故意把 table store 行的 meta_type 改成 -1，但保持 value 不变，
  // 模拟备份恢复后类型字段无效的情况。上传时应该只处理 tablet 真实引用的 sub-meta，
  // 不能把目录里的旧对象一起上传，而且仍能按 tablet 的真实引用识别 table store。
  ASSERT_EQ(OB_SUCCESS, update_sys_tiered_meta_type(table_store_macro_id, -1));
  bool table_store_meta_type_needs_restore = true;
  auto table_store_meta_type_guard = MAKE_SCOPE(
      table_store_meta_type_needs_restore, [&](bool &needs_restore) {
        if (needs_restore) {
          const int tmp_ret = update_sys_tiered_meta_type(
              table_store_macro_id, static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE));
          if (OB_SUCCESS != tmp_ret) {
            ADD_FAILURE() << "恢复 table store 行的 meta_type 失败, ret=" << tmp_ret;
          }
        }
      });
  // 这里故意只调用一级 tablet 上传接口；该接口内部会复用刚反序列化出的 tablet，
  // 找到并上传它引用的 sub-meta，然后再写入转换后的 tablet meta。
  ObSSLsCkptData tablet_ckpt_data;
  ASSERT_EQ(OB_SUCCESS, upload_tablet_meta_for_test(
      ckpt_writer, tablet_key, tablet_ckpt_data));
  ASSERT_EQ(nullptr, ckpt_writer.allocator_);
  ASSERT_EQ(nullptr, ckpt_writer.tiered_store_);
  ASSERT_EQ(nullptr, ckpt_writer.tablet_);
  ASSERT_EQ(nullptr, ckpt_writer.macro_info_);
  ASSERT_EQ(nullptr, ckpt_writer.tablet_meta_.tablet_);
  ASSERT_EQ(nullptr, ckpt_writer.tablet_meta_.macro_info_);
  ASSERT_EQ(0, ckpt_writer.tablet_meta_.data_version_);
  ASSERT_FALSE(ckpt_writer.tablet_key_.is_valid());
  // 不只检查一个对象：当前 tablet 引用的 table store、schema 和各类 SSTable
  // 都应该由同一次引用驱动上传完整，旧目录项仍然不能被带上去。
  for (int64_t i = 0; i < referenced_ids.count(); ++i) {
    blocksstable::MacroBlockId referenced_oss_id = referenced_ids.at(i);
    referenced_oss_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
    bool referenced_oss_exist = false;
    ASSERT_EQ(OB_SUCCESS, blocksstable::ObObjectManager::ss_is_exist_object(
        referenced_oss_id, 0/*ls_epoch*/, referenced_oss_exist));
    ASSERT_TRUE(referenced_oss_exist);
  }

  blocksstable::MacroBlockId stale_oss_id = stale_id;
  stale_oss_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
  bool stale_oss_exist = false;
  ASSERT_EQ(OB_SUCCESS,
      blocksstable::ObObjectManager::ss_is_exist_object(stale_oss_id, 0/*ls_epoch*/, stale_oss_exist));
  ASSERT_FALSE(stale_oss_exist);

  ASSERT_EQ(1, tablet_ckpt_data.tablet_meta_keys_.count());
  ObSSLsCkptReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init());
  reader.ckpt_data_.ls_meta_.ls_id_ = ls_id;
  reader.ckpt_data_.ckpt_scn_ = tablet_meta_scn;
  reader.ckpt_data_.reserved_meta_scn_ = tablet_meta_scn;
  ASSERT_EQ(OB_SUCCESS, reader.ckpt_data_.tablet_meta_keys_.assign(
      tablet_ckpt_data.tablet_meta_keys_));
  ASSERT_EQ(OB_SUCCESS, reader.build_tablet_meta_set_());
  ASSERT_TRUE(reader.contains_tablet_meta_(
      tablet_ckpt_data.tablet_meta_keys_.at(0)));
  reader.is_loaded_ = true;

  ObArenaAllocator reader_allocator("CkptReader");
  ObTabletHandle restored_tablet_handle;
  ObSSUpdateTabletLog restored_update_log;
  ASSERT_EQ(OB_SUCCESS, reader.read_tablet(
      tablet_ckpt_data.tablet_meta_keys_.at(0), reader_allocator,
      restored_tablet_handle, restored_update_log));
  ASSERT_TRUE(restored_tablet_handle.is_valid());
  ASSERT_EQ(tablet_id, restored_tablet_handle.get_obj()->get_tablet_id());
  ASSERT_TRUE(restored_update_log.is_valid());
  ASSERT_EQ(ls_id, restored_update_log.ls_id_);
  ASSERT_EQ(tablet_id, restored_update_log.tablet_id_);
  ASSERT_EQ(reorgan_scn, restored_update_log.reorg_scn_);
  ASSERT_TRUE(restored_tablet_handle.get_obj()->table_store_addr_.is_disk_object());
  ASSERT_TRUE(restored_tablet_handle.get_obj()->storage_schema_addr_.is_disk_object());
  blocksstable::MacroBlockId restored_table_store_id;
  blocksstable::MacroBlockId restored_storage_schema_id;
  ASSERT_EQ(OB_SUCCESS, restored_tablet_handle.get_obj()->table_store_addr_.addr_.get_macro_block_id(
      restored_table_store_id));
  ASSERT_EQ(OB_SUCCESS, restored_tablet_handle.get_obj()->storage_schema_addr_.addr_.get_macro_block_id(
      restored_storage_schema_id));
  // reader 没有再做地址重定向；如果 writer 没有提前转换，这两个断言会看到 TABLE 地址。
  ASSERT_TRUE(restored_table_store_id.is_shared_tablet_sub_meta());
  ASSERT_TRUE(restored_storage_schema_id.is_shared_tablet_sub_meta());
  ASSERT_EQ(OB_SUCCESS, update_sys_tiered_meta_type(
      table_store_macro_id, static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE)));
  table_store_meta_type_needs_restore = false;
  // 测试中途手工写入的旧 sub-meta 不能留在测试环境里。
  ASSERT_EQ(OB_SUCCESS, tiered_store.del_meta(stale_id));
  stale_meta_needs_cleanup = false;
  LOG_INFO("[TEST CASE END] sys_ls_ckpt_uploads_only_referenced_sub_meta",
           K(tablet_key), K(stale_id), K(referenced_ids.count()));
}

/**
 * 概要：普通 checkpoint 的 tablet meta 和实际引用的 sub-meta，只有在完整
 * payload 一致时才能幂等跳过；bootstrap 关闭上层存在性检查后仍遵循底层写入规则。
 *
 * 覆盖模块：
 * 1. ObSSLsCkptWriter：普通 checkpoint 遇到同名 tablet meta/sub-meta 时先回读
 *    并比较内容；bootstrap writer 支持重新创建缺失对象和同内容幂等重传。
 * 2. 内容演进靠新 meta_scn 落到新对象 id，与旧对象互不干扰。
 * 3. read_tablet：只允许读取当前 checkpoint 清单中的 tablet 对象。
 *
 * 测试流程：
 * 1. 场景1：正常上传一次，OSS 上已有一个对象。
 * 2. 场景2：相同输入重传，落盘对齐内容完全一致，幂等成功。
 * 3. 场景3：埋入旧格式/截断 payload，再重传必须报长度冲突。
 * 4. 场景4：埋入同长度错误字节，再重传必须报内容冲突。
 * 5. 场景5：测试删除冲突 tablet meta 后，bootstrap writer 重建对象并幂等重传。
 * 6. 场景6：逐个破坏当前 tablet 实际引用的 sub-meta；普通 writer 必须拒绝，
 *    测试删除冲突对象后，bootstrap writer 必须重新创建并恢复原始内容。
 * 7. 场景7：换一个更大的 meta_scn（新对象 id），重传成功；
 *    新 checkpoint 能读新对象，但必须拒绝不在其清单中的旧 checkpoint 对象。
 */
TEST_F(TestSharedMetaService, sys_ls_ckpt_rejects_conflicting_oss_object)
{
  LOG_INFO("[TEST CASE BEGIN] sys_ls_ckpt_rejects_conflicting_oss_object");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  INIT_SYS_TIERED_STORE_V2(tiered_store);

  const int64_t table_suffix = ObTimeUtility::current_time();
  ObSqlString table_name;
  ObSqlString create_table_sql;
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("create database if not exists ss_meta_test"));
  ASSERT_EQ(OB_SUCCESS, table_name.assign_fmt("sys_ls_ckpt_overwrite_%ld", table_suffix));
  ASSERT_EQ(OB_SUCCESS, create_table_sql.assign_fmt(
      "create table ss_meta_test.%s (a int)", table_name.ptr()));
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(create_table_sql.ptr()));

  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_sys_tablet_and_ls_id(table_name.ptr(), tablet_id, ls_id));
  ASSERT_TRUE(tablet_id.is_valid());
  ASSERT_TRUE(ls_id.is_valid());

  const share::SCN reorgan_scn = share::SCN::min_scn();
  CLEAN_SYS_TABLET_META(meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn);
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(
      ls_id, tablet_id, reorgan_scn, true/*check_lease*/));

  ObArenaAllocator allocator("CkptOverwrite");
  ObTabletHandle tablet_handle;
  share::SCN tablet_meta_scn;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(
      ls_id, tablet_id, reorgan_scn, allocator, tablet_handle, tablet_meta_scn));
  ASSERT_TRUE(tablet_handle.is_valid());
  ASSERT_TRUE(tablet_meta_scn.is_valid());

  ObSSMetaTabletKey tablet_key;
  tablet_key.ls_id_ = ls_id;
  tablet_key.tablet_id_ = tablet_id;
  tablet_key.reorganization_scn_ = reorgan_scn;
  tablet_key.meta_scn_ = tablet_meta_scn;

  ObTabletMacroInfo *tablet_macro_info = nullptr;
  ObArray<blocksstable::MacroBlockId> referenced_sub_meta_oss_ids;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->load_macro_info(
      0 /*ls_epoch*/, allocator, tablet_macro_info));
  ASSERT_NE(nullptr, tablet_macro_info);
  for (int64_t i = 0; i < tablet_macro_info->shared_meta_block_info_arr_.cnt_; ++i) {
    blocksstable::MacroBlockId sub_meta_id =
        tablet_macro_info->shared_meta_block_info_arr_.arr_[i];
    if (sub_meta_id.is_shared_tablet_sub_meta_in_table()) {
      sub_meta_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
      ASSERT_TRUE(sub_meta_id.is_shared_tablet_sub_meta());
      ASSERT_EQ(OB_SUCCESS, referenced_sub_meta_oss_ids.push_back(sub_meta_id));
    }
  }
  ASSERT_GE(referenced_sub_meta_oss_ids.count(), 1);

  // ---------- 场景 1：先正常上传一次，OSS 上已有同名对象 ----------
  ObSSLsCkptWriter ckpt_writer(true /*need_check_existing_object*/);
  ObSSLsCkptData ckpt_data;
  ASSERT_EQ(OB_SUCCESS, upload_tablet_meta_for_test(ckpt_writer, tablet_key, ckpt_data));
  ASSERT_EQ(1, ckpt_data.tablet_meta_keys_.count());
  const blocksstable::MacroBlockId tablet_meta_oss_id = ckpt_data.tablet_meta_keys_.at(0);
  ASSERT_TRUE(tablet_meta_oss_id.is_in_object_storage());

  // ---------- 场景 2：相同 payload 重传应幂等成功 ----------
  ObSSLsCkptData idempotent_ckpt_data;
  ASSERT_EQ(OB_SUCCESS, upload_tablet_meta_for_test(
      ckpt_writer, tablet_key, idempotent_ckpt_data));
  ASSERT_EQ(1, idempotent_ckpt_data.tablet_meta_keys_.count());
  ASSERT_EQ(tablet_meta_oss_id, idempotent_ckpt_data.tablet_meta_keys_.at(0));

  // ---------- 场景 3：同名对象长度冲突时必须失败且不覆盖 ----------
  ObArenaAllocator conflict_allocator("CkptConflict");
  ObString original_content;
  ASSERT_EQ(OB_SUCCESS, ObSSLsCkptReader::read_object_(
      tablet_meta_oss_id, conflict_allocator, original_content));
  ASSERT_GT(original_content.length(), 0);
  int64_t object_size = 0;
  auto overwrite_object = [&](const blocksstable::MacroBlockId &object_id,
                              const ObString &content) -> void {
    ASSERT_EQ(OB_SUCCESS, delete_test_object(object_id));
    blocksstable::ObStorageObjectWriteInfo write_info;
    blocksstable::ObStorageObjectHandle write_handle;
    write_info.buffer_ = content.ptr();
    write_info.offset_ = 0;
    write_info.size_ = content.length();
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_desc_.set_unsealed();
    write_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.set_ls_epoch_id(0);
    ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.async_write_object(
        object_id, write_info, write_handle));
    ASSERT_EQ(OB_SUCCESS, write_handle.wait());
  };
  auto assert_object_content = [&](const blocksstable::MacroBlockId &object_id,
                                   const ObString &expected_content) -> void {
    // 本用例故意覆写同一对象 ID；sub-meta 缓存仍可能保留旧内容。
    // 校验远端对象未被覆盖或已恢复，必须和 writer 一样绕过缓存读取。
    ObBaseFileManager *file_manager = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::get_file_manager(MTL_ID(), file_manager));
    ASSERT_NE(nullptr, file_manager);
    ASSERT_EQ(OB_SUCCESS, file_manager->get_remote_file_length(
        object_id, 0 /*ls_epoch*/, object_size));
    ASSERT_EQ(expected_content.length(), object_size);
    ASSERT_GT(object_size, 0);
    ObArenaAllocator read_allocator("CkptConflictRd");
    char *read_buf = static_cast<char *>(read_allocator.alloc(object_size));
    ASSERT_NE(nullptr, read_buf);
    blocksstable::ObStorageObjectReadInfo read_info;
    blocksstable::ObStorageObjectHandle read_handle;
    ObSSObjectStorageReader object_storage_reader;
    read_info.macro_block_id_ = object_id;
    read_info.offset_ = 0;
    read_info.size_ = object_size;
    read_info.buf_ = read_buf;
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    read_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID());
    read_info.set_ls_epoch_id(0 /*ls_epoch_id*/);
    read_info.mtl_tenant_id_ = MTL_ID();
    ASSERT_EQ(OB_SUCCESS, object_storage_reader.aio_read(read_info, read_handle));
    ASSERT_EQ(OB_SUCCESS, read_handle.wait());
    ASSERT_NE(nullptr, read_handle.get_buffer());
    ASSERT_EQ(expected_content.length(), read_handle.get_data_size());
    ASSERT_EQ(0, MEMCMP(
        expected_content.ptr(), read_buf, expected_content.length()));
  };

  // 用短的旧格式字节模拟历史 payload 或写失败后的截断残留。
  const ObString legacy_content = ObString::make_string("legacy-tablet-meta-v0");
  ASSERT_LT(legacy_content.length(), original_content.length());
  overwrite_object(tablet_meta_oss_id, legacy_content);
  ObSSLsCkptData legacy_reupload_ckpt_data;
  ASSERT_EQ(OB_OBJECT_STORAGE_CONDITION_NOT_MATCH,
      upload_tablet_meta_for_test(ckpt_writer, tablet_key, legacy_reupload_ckpt_data));
  ASSERT_EQ(nullptr, ckpt_writer.allocator_);
  ASSERT_EQ(nullptr, ckpt_writer.tiered_store_);
  ASSERT_EQ(nullptr, ckpt_writer.tablet_);
  ASSERT_EQ(nullptr, ckpt_writer.macro_info_);
  ASSERT_EQ(nullptr, ckpt_writer.tablet_meta_.tablet_);
  ASSERT_EQ(nullptr, ckpt_writer.tablet_meta_.macro_info_);
  ASSERT_EQ(0, ckpt_writer.tablet_meta_.data_version_);
  ASSERT_FALSE(ckpt_writer.tablet_key_.is_valid());
  ASSERT_EQ(0, legacy_reupload_ckpt_data.tablet_meta_keys_.count());
  assert_object_content(tablet_meta_oss_id, legacy_content);

  // 恢复正确对象后，同一版本仍应能幂等通过。
  overwrite_object(tablet_meta_oss_id, original_content);
  ObSSLsCkptData restored_ckpt_data;
  ASSERT_EQ(OB_SUCCESS, upload_tablet_meta_for_test(
      ckpt_writer, tablet_key, restored_ckpt_data));
  ASSERT_EQ(1, restored_ckpt_data.tablet_meta_keys_.count());

  // ---------- 场景 4：同名对象内容冲突时必须失败且不覆盖 ----------
  char *conflicting_buf = static_cast<char *>(
      conflict_allocator.alloc(original_content.length()));
  ASSERT_NE(nullptr, conflicting_buf);
  MEMCPY(conflicting_buf, original_content.ptr(), original_content.length());
  conflicting_buf[0] ^= 0x1;
  const ObString conflicting_content(
      original_content.length(), conflicting_buf);
  overwrite_object(tablet_meta_oss_id, conflicting_content);

  // 重传同一版本：发现错误 payload 后禁止把该对象加入 checkpoint。
  ObSSLsCkptData reupload_ckpt_data;
  ASSERT_EQ(OB_OBJECT_STORAGE_CONDITION_NOT_MATCH,
      upload_tablet_meta_for_test(ckpt_writer, tablet_key, reupload_ckpt_data));
  ASSERT_EQ(0, reupload_ckpt_data.tablet_meta_keys_.count());

  assert_object_content(tablet_meta_oss_id, conflicting_content);

  // ---------- Case 5: bootstrap recreates missing objects and supports idempotent uploads ----------
  // Skipping the writer's existence check does not bypass the storage backend's conditional writes.
  ASSERT_EQ(OB_SUCCESS, delete_test_object(tablet_meta_oss_id));
  ObSSLsCkptWriter bootstrap_writer(false /*need_check_existing_object*/);
  ObSSLsCkptData bootstrap_ckpt_data;
  ASSERT_FALSE(bootstrap_writer.need_check_existing_object_);
  ASSERT_EQ(OB_SUCCESS, upload_tablet_meta_for_test(
      bootstrap_writer, tablet_key, bootstrap_ckpt_data));
  ASSERT_EQ(1, bootstrap_ckpt_data.tablet_meta_keys_.count());
  ASSERT_EQ(tablet_meta_oss_id, bootstrap_ckpt_data.tablet_meta_keys_.at(0));
  assert_object_content(tablet_meta_oss_id, original_content);

  ObSSLsCkptData bootstrap_idempotent_ckpt_data;
  ASSERT_EQ(OB_SUCCESS, upload_tablet_meta_for_test(
      bootstrap_writer, tablet_key, bootstrap_idempotent_ckpt_data));
  ASSERT_EQ(1, bootstrap_idempotent_ckpt_data.tablet_meta_keys_.count());
  ASSERT_EQ(tablet_meta_oss_id, bootstrap_idempotent_ckpt_data.tablet_meta_keys_.at(0));
  assert_object_content(tablet_meta_oss_id, original_content);

  // Bootstrap restored the object, so a normal checkpoint upload should be idempotent again.
  ObSSLsCkptData final_idempotent_ckpt_data;
  ASSERT_EQ(OB_SUCCESS, upload_tablet_meta_for_test(
      ckpt_writer, tablet_key, final_idempotent_ckpt_data));
  ASSERT_EQ(1, final_idempotent_ckpt_data.tablet_meta_keys_.count());

  // ---------- Case 6: every referenced sub-meta follows the same conflict policy ----------
  for (int64_t i = 0; i < referenced_sub_meta_oss_ids.count(); ++i) {
    const blocksstable::MacroBlockId &sub_meta_oss_id = referenced_sub_meta_oss_ids.at(i);
    ObArenaAllocator sub_meta_allocator("CkptSubConflict");
    ObString original_sub_meta_content;
    ASSERT_EQ(OB_SUCCESS, ObSSLsCkptReader::read_object_(
        sub_meta_oss_id, sub_meta_allocator, original_sub_meta_content));
    ASSERT_GT(original_sub_meta_content.length(), 0);

    char *conflicting_sub_meta_buf = static_cast<char *>(
        sub_meta_allocator.alloc(original_sub_meta_content.length()));
    ASSERT_NE(nullptr, conflicting_sub_meta_buf);
    MEMCPY(conflicting_sub_meta_buf, original_sub_meta_content.ptr(),
           original_sub_meta_content.length());
    conflicting_sub_meta_buf[0] ^= 0x1;
    const ObString conflicting_sub_meta_content(
        original_sub_meta_content.length(), conflicting_sub_meta_buf);
    overwrite_object(sub_meta_oss_id, conflicting_sub_meta_content);

    ObSSLsCkptData sub_meta_conflict_ckpt_data;
    ASSERT_EQ(OB_OBJECT_STORAGE_CONDITION_NOT_MATCH,
        upload_tablet_meta_for_test(
            ckpt_writer, tablet_key, sub_meta_conflict_ckpt_data));
    ASSERT_TRUE(sub_meta_conflict_ckpt_data.tablet_meta_keys_.empty());
    assert_object_content(sub_meta_oss_id, conflicting_sub_meta_content);

    // Remove the conflicting fixture before asking bootstrap to recreate the sub-meta.
    ASSERT_EQ(OB_SUCCESS, delete_test_object(sub_meta_oss_id));
    ObSSLsCkptData sub_meta_bootstrap_ckpt_data;
    ASSERT_EQ(OB_SUCCESS, upload_tablet_meta_for_test(
        bootstrap_writer, tablet_key, sub_meta_bootstrap_ckpt_data));
    ASSERT_EQ(1, sub_meta_bootstrap_ckpt_data.tablet_meta_keys_.count());
    ASSERT_EQ(tablet_meta_oss_id, sub_meta_bootstrap_ckpt_data.tablet_meta_keys_.at(0));
    assert_object_content(sub_meta_oss_id, original_sub_meta_content);
  }

  // ---------- 场景 7：新 meta_scn 使用新对象 id，不受旧对象冲突影响 ----------
  // 同一份 payload，人为换一个大一号的 scn，转换出的 macro id 应不同。
  ObSSMetaTabletKey key_v2 = tablet_key;
  key_v2.meta_scn_ = share::SCN::scn_inc(tablet_meta_scn);
  ASSERT_GT(key_v2.meta_scn_, tablet_key.meta_scn_);
  // read_tablet_meta 按 (ls_id, tablet_id, reorgan_scn, meta_scn) 精确查表；
  // 要让 upload_tablet_meta_ 用 key_v2 的 meta_scn 读到 payload，需要先把
  // 同一份 row 以 key_v2 的 meta_scn 写入 __all_shared_meta 表，
  // 否则 read_tablet_meta(key_v2) 会拿到 OB_ENTRY_NOT_EXIST。
  {
    ObSSMetaTabletRow origin_row;
    ASSERT_EQ(OB_SUCCESS, meta_svr->read_tablet_meta(tablet_key, allocator, origin_row));
    ObSSMetaTabletRow v2_row(
        static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META), key_v2, origin_row.value_);
    ObSEArray<ObSSMetaTabletRow, SSMETA_ROW_BATCH_CNT> v2_rows;
    ASSERT_EQ(OB_SUCCESS, v2_rows.push_back(v2_row));
    ASSERT_EQ(OB_SUCCESS, meta_svr->batch_insert_tablet_metas(v2_rows));
  }
  ObSSLsCkptData ckpt_v2;
  ASSERT_EQ(OB_SUCCESS, upload_tablet_meta_for_test(ckpt_writer, key_v2, ckpt_v2));
  ASSERT_EQ(1, ckpt_v2.tablet_meta_keys_.count());
  const blocksstable::MacroBlockId oss_id_v2 = ckpt_v2.tablet_meta_keys_.at(0);
  ASSERT_TRUE(oss_id_v2.is_in_object_storage());
  ASSERT_NE(tablet_meta_oss_id, oss_id_v2);

  // 新 checkpoint 只包含 v2。v1 对象仍然存在且 SCN 更小，但不属于
  // 当前 checkpoint，reader 不能绕过 checkpoint 清单直接读取它。
  ObSSLsCkptReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init());
  reader.ckpt_data_.ls_meta_.ls_id_ = ls_id;
  reader.ckpt_data_.ckpt_scn_ = key_v2.meta_scn_;
  reader.ckpt_data_.reserved_meta_scn_ = key_v2.meta_scn_;
  ASSERT_EQ(OB_SUCCESS, reader.ckpt_data_.tablet_meta_keys_.push_back(oss_id_v2));
  ASSERT_EQ(OB_SUCCESS, reader.build_tablet_meta_set_());
  ASSERT_EQ(1, reader.tablet_meta_set_.size());
  ASSERT_FALSE(reader.contains_tablet_meta_(tablet_meta_oss_id));
  ASSERT_TRUE(reader.contains_tablet_meta_(oss_id_v2));
  reader.is_loaded_ = true;
  ObArenaAllocator read_allocator("CkptScnRd");
  ObTabletHandle read_handle;
  ObSSUpdateTabletLog read_log;
  ASSERT_EQ(OB_INVALID_DATA, reader.read_tablet(
      tablet_meta_oss_id, read_allocator, read_handle, read_log));
  ASSERT_FALSE(read_handle.is_valid());
  ASSERT_FALSE(read_log.is_valid());

  ASSERT_EQ(OB_SUCCESS, reader.read_tablet(
      oss_id_v2, read_allocator, read_handle, read_log));
  ASSERT_TRUE(read_handle.is_valid());
  ASSERT_NE(nullptr, read_handle.get_obj());
  ASSERT_EQ(tablet_id, read_handle.get_obj()->get_tablet_id());

  LOG_INFO("[TEST CASE END] sys_ls_ckpt_rejects_conflicting_oss_object",
           K(tablet_key), K(tablet_meta_oss_id), K(object_size),
           "sub_meta_count", referenced_sub_meta_oss_ids.count());
}

// tablet meta key 来自 checkpoint 内部清单：转换前必须先验证全部入参，
// 通过入参校验后的转换失败透传底层错误码，与 checkpoint 不匹配才是非预期状态。
TEST(ObSSLsCkptReaderTest, parse_tablet_meta_key_validates_id_before_conversion)
{
  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  ObSSMetaTabletKey tablet_key;
  tablet_key.ls_id_ = ls_id;
  tablet_key.tablet_id_ = ObTabletID(1001);
  tablet_key.reorganization_scn_ = share::SCN::min_scn();
  tablet_key.meta_scn_ = share::SCN::base_scn();
  ASSERT_TRUE(tablet_key.is_valid());

  ObSSLsCkptReader reader;
  // The object is published at ckpt_scn_, but its metadata snapshot only
  // covers reserved_meta_scn_. A tablet visible at C but newer than R must fail.
  reader.ckpt_data_.reserved_meta_scn_ = tablet_key.meta_scn_;
  reader.ckpt_data_.ckpt_scn_ = share::SCN::scn_inc(tablet_key.meta_scn_);
  const blocksstable::MacroBlockId object_id =
      tablet_key.convert_to_macro_id(false /*is_in_table*/);
  ObSSMetaTabletKey parsed_key;
  ASSERT_EQ(OB_SUCCESS, reader.parse_tablet_meta_key_(ls_id, object_id, parsed_key));
  ASSERT_TRUE(tablet_key == parsed_key);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      reader.parse_tablet_meta_key_(ObLSID(), object_id, parsed_key));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      reader.parse_tablet_meta_key_(ls_id, blocksstable::MacroBlockId(), parsed_key));

  blocksstable::MacroBlockId local_id(
      static_cast<uint64_t>(blocksstable::ObStorageObjectType::SHARED_TABLET_META),
      tablet_key.tablet_id_.id(), 0);
  ASSERT_TRUE(local_id.is_valid());
  ASSERT_TRUE(local_id.is_id_mode_local());
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      reader.parse_tablet_meta_key_(ls_id, local_id, parsed_key));

  const blocksstable::MacroBlockId table_id =
      tablet_key.convert_to_macro_id(true /*is_in_table*/);
  ASSERT_TRUE(is_valid_ss_tablet_id(table_id));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      reader.parse_tablet_meta_key_(ls_id, table_id, parsed_key));
  ASSERT_FALSE(parsed_key.is_valid());

  blocksstable::MacroBlockId invalid_scn_id(object_id);
  invalid_scn_id.set_meta_scn(INT64_MAX);
  ASSERT_TRUE(is_valid_ss_tablet_id(invalid_scn_id));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      reader.parse_tablet_meta_key_(ls_id, invalid_scn_id, parsed_key));
  ASSERT_FALSE(parsed_key.is_valid());

  ObSSMetaTabletKey inner_tablet_key;
  inner_tablet_key.ls_id_ = ObLSID(2);
  inner_tablet_key.tablet_id_ = ObTabletID(ObTabletID::LS_TX_DATA_TABLET_ID);
  inner_tablet_key.reorganization_scn_ = share::SCN::min_scn();
  inner_tablet_key.meta_scn_ = tablet_key.meta_scn_;
  ASSERT_TRUE(inner_tablet_key.is_valid());
  const blocksstable::MacroBlockId mismatched_ls_id =
      inner_tablet_key.convert_to_macro_id(false /*is_in_table*/);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      reader.parse_tablet_meta_key_(ls_id, mismatched_ls_id, parsed_key));

  ObSSMetaTabletKey newer_tablet_key = tablet_key;
  newer_tablet_key.meta_scn_ = share::SCN::scn_inc(tablet_key.meta_scn_);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      reader.parse_tablet_meta_key_(
          ls_id, newer_tablet_key.convert_to_macro_id(false /*is_in_table*/), parsed_key));
}

// 重载 checkpoint 时必须重建 tablet meta 哈希索引，旧 checkpoint 的对象
// 不能残留在新索引中；空 checkpoint 也要保持一个可查询的空索引。
TEST(ObSSLsCkptReaderTest, tablet_meta_index_rebuild_clears_previous_checkpoint)
{
  ObSSMetaTabletKey first_key;
  first_key.ls_id_ = ObLSID(ObLSID::SYS_LS_ID);
  first_key.tablet_id_ = ObTabletID(1001);
  first_key.reorganization_scn_ = share::SCN::min_scn();
  first_key.meta_scn_ = share::SCN::base_scn();
  ASSERT_TRUE(first_key.is_valid());
  ObSSMetaTabletKey second_key = first_key;
  second_key.tablet_id_ = ObTabletID(1002);
  ASSERT_TRUE(second_key.is_valid());
  const blocksstable::MacroBlockId first_id =
      first_key.convert_to_macro_id(false /*is_in_table*/);
  const blocksstable::MacroBlockId second_id =
      second_key.convert_to_macro_id(false /*is_in_table*/);

  ObSSLsCkptReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init());
  ASSERT_EQ(OB_SUCCESS, reader.ckpt_data_.tablet_meta_keys_.push_back(first_id));
  ASSERT_EQ(OB_SUCCESS, reader.build_tablet_meta_set_());
  ASSERT_EQ(1, reader.tablet_meta_set_.size());
  ASSERT_TRUE(reader.contains_tablet_meta_(first_id));
  ASSERT_FALSE(reader.contains_tablet_meta_(second_id));

  reader.ckpt_data_.tablet_meta_keys_.reset();
  ASSERT_EQ(OB_SUCCESS, reader.ckpt_data_.tablet_meta_keys_.push_back(second_id));
  ASSERT_EQ(OB_SUCCESS, reader.build_tablet_meta_set_());
  ASSERT_EQ(1, reader.tablet_meta_set_.size());
  ASSERT_FALSE(reader.contains_tablet_meta_(first_id));
  ASSERT_TRUE(reader.contains_tablet_meta_(second_id));

  reader.ckpt_data_.tablet_meta_keys_.reset();
  ASSERT_EQ(OB_SUCCESS, reader.build_tablet_meta_set_());
  ASSERT_TRUE(reader.tablet_meta_set_.created());
  ASSERT_TRUE(reader.tablet_meta_set_.empty());
  ASSERT_FALSE(reader.contains_tablet_meta_(second_id));

  reader.destroy();
  ASSERT_FALSE(reader.tablet_meta_set_.created());
}

/**
 * 概要：把 SYS tablet 的 checkpoint 写到对象存储，再读回来重建本地 tablet，
 * 用“恢复前后逐行比对真实数据”来证明恢复结果与 checkpoint 点位一致，
 * 并且恢复出来的 tablet 还能继续正常写入。
 *
 * 覆盖模块：
 * 1. ObSSLsCkptWriter：将真实 SYS tablet 及其引用的
 *    sub-meta（含 major / minor sstable）写入对象存储，并生成 LS checkpoint。
 * 2. ObSSLsCkptReader：从 checkpoint 文件读回 shared tablet。
 * 3. ObLS：通过 create_or_update_with_ss_tablet 重建本地 PRIVATE_TABLET_META。
 *
 * 测试流程：
 * 1. 建表后先写数据、分别做 minor / major freeze，让 tablet 同时带上
 *    major 和 minor 两类 sstable，再记下此刻的整表内容。
 * 2. 备份这个版本（写 checkpoint），用 Reader 读回，重建本地私有 tablet。
 * 3. 恢复后先不写，直接整表比对，确认与 checkpoint 点位完全一致。
 * 4. 再插入/修改几行，再次整表比对，确认恢复出的 tablet 可继续读写。
 *
 * 该用例覆盖 checkpoint 写入、读取、私有 tablet 重建和 SQL 读写的完整端到端链路。
 */
TEST_F(TestSharedMetaService, sys_ls_ckpt_restores_working_private_tablet)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[TEST CASE BEGIN] sys_ls_ckpt_restores_working_private_tablet");
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObLSService *ls_svr = MTL(ObLSService *);
  ObTenantFileManager *file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, meta_svr);
  ASSERT_NE(nullptr, ls_svr);
  ASSERT_NE(nullptr, file_mgr);
  INIT_SYS_TIERED_STORE_V2(tiered_store);

  // 读某列整表内容到 (key -> value) 映射，用于恢复前后逐行比对。
  auto dump_rows = [&](const char *tbl, std::map<int64_t, int64_t> &rows) {
    rows.clear();
    ObSqlString q;
    ASSERT_EQ(OB_SUCCESS, q.assign_fmt("select a, b from ss_meta_test.%s order by a", tbl));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().read(res, q.ptr()));
      sqlclient::ObMySQLResult *r = res.get_result();
      ASSERT_NE(nullptr, r);
      while (true) {
        ret = r->next();
        if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
        ASSERT_EQ(OB_SUCCESS, ret);
        int64_t k = -1, v = -1;
        ASSERT_EQ(OB_SUCCESS, r->get_int("a", k));
        ASSERT_EQ(OB_SUCCESS, r->get_int("b", v));
        rows[k] = v;
      }
    }
  };

  const int64_t table_suffix = ObTimeUtility::current_time();
  ObSqlString table_name;
  ObSqlString create_table_sql;
  ObSqlString drop_table_sql;
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("create database if not exists ss_meta_test"));
  ASSERT_EQ(OB_SUCCESS, table_name.assign_fmt("sys_ls_ckpt_restore_%ld", table_suffix));
  ASSERT_EQ(OB_SUCCESS, create_table_sql.assign_fmt(
      "create table ss_meta_test.%s (a int primary key, b int)", table_name.ptr()));
  ASSERT_EQ(OB_SUCCESS, drop_table_sql.assign_fmt(
      "drop table if exists ss_meta_test.%s", table_name.ptr()));
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(create_table_sql.ptr()));

  // 用例中途失败时也不遗留测试表。本 guard 先声明，
  // 后面的本地 tablet 恢复 guard 会先于它执行。
  bool table_needs_drop = true;
  auto drop_table_guard = MAKE_SCOPE(table_needs_drop, [&](bool &needs_drop) {
    if (needs_drop) {
      const int tmp_ret = sys_exe_sql(drop_table_sql.ptr());
      if (OB_SUCCESS != tmp_ret) {
        ADD_FAILURE() << "清理 checkpoint 恢复测试表失败, ret=" << tmp_ret;
      }
    }
  });

  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_sys_tablet_and_ls_id(table_name.ptr(), tablet_id, ls_id));
  ASSERT_TRUE(tablet_id.is_valid());
  ASSERT_TRUE(ls_id.is_valid());
  ASSERT_TRUE(ls_id.is_sys_ls());

  const share::SCN reorgan_scn = share::SCN::min_scn();

  // ---------- 步骤 1：造数据，并分别产出 minor 与 major sstable ----------
  // minor sstable 要等 inc upload 任务才会进入 shared meta；默认 10 分钟一次太慢，
  // 先把上传间隔调到 1s、上传并发调高，避免后面轮询一直等不到。
  // compaction_schedule_interval 默认 120s，debug 环境下 major merge 调度太慢，
  // 调到合法下限 3s，让 freeze 后尽快触发 merge。
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(
      "alter system set ob_compaction_schedule_interval = '3s'"));
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(
      "alter system set _shared_inc_sstable_upload_interval = '1s'"));
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(
      "alter system set inc_sstable_upload_thread_score = 20"));
  // 先写 10 行并 minor freeze，让这部分数据落进 minor sstable。
  for (int64_t i = 1; i <= 10; ++i) {
    ObSqlString ins;
    ASSERT_EQ(OB_SUCCESS, ins.assign_fmt(
        "insert into ss_meta_test.%s values (%ld, %ld)", table_name.ptr(), i, i * 10));
    ASSERT_EQ(OB_SUCCESS, sys_exe_sql(ins.ptr()));
  }
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("alter system minor freeze"));

  // 再改 5 行 + 新增 5 行并 major freeze，把它们合并进 major sstable。
  // major 会压实前面的 minor，checkpoint 版本因此同时带 major 和 minor 两类引用。
  for (int64_t i = 1; i <= 5; ++i) {
    ObSqlString upd;
    ASSERT_EQ(OB_SUCCESS, upd.assign_fmt(
        "update ss_meta_test.%s set b = %ld where a = %ld", table_name.ptr(), i * 100, i));
    ASSERT_EQ(OB_SUCCESS, sys_exe_sql(upd.ptr()));
  }
  for (int64_t i = 11; i <= 15; ++i) {
    ObSqlString ins;
    ASSERT_EQ(OB_SUCCESS, ins.assign_fmt(
        "insert into ss_meta_test.%s values (%ld, %ld)", table_name.ptr(), i, i * 10));
    ASSERT_EQ(OB_SUCCESS, sys_exe_sql(ins.ptr()));
  }
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("alter system major freeze"));

  // 最后再补一个 minor（仅 minor freeze，不 major），保证 major 之上还有一层最新 minor。
  for (int64_t i = 16; i <= 18; ++i) {
    ObSqlString ins;
    ASSERT_EQ(OB_SUCCESS, ins.assign_fmt(
        "insert into ss_meta_test.%s values (%ld, %ld)", table_name.ptr(), i, i * 10));
    ASSERT_EQ(OB_SUCCESS, sys_exe_sql(ins.ptr()));
  }
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("alter system minor freeze"));

  // 等 tablet 的 shared meta 同时出现 major 和 minor sstable，确保 checkpoint
  // 备份到的是“有真实数据、且含两类 sstable”的版本，而不是空壳。
  // major merge 在 debug 环境下可能很慢，这里给足 10 分钟。
  const int64_t wait_timeout_us = 600 * 1000 * 1000L;
  const int64_t wait_start_us = ObTimeUtility::current_time();
  bool major_minor_ready = false;
  while (!major_minor_ready && ObTimeUtility::current_time() - wait_start_us < wait_timeout_us) {
    ObArenaAllocator probe_alloc("CkptProbe");
    ObTabletHandle probe_handle;
    if (OB_SUCCESS == meta_svr->get_tablet(
            ls_id, tablet_id, reorgan_scn, probe_alloc, probe_handle)
        && probe_handle.is_valid()) {
      // 从 shared meta 读取的 tablet，其 table_store_addr_.ptr_ 可能为 null
      // （table store 尚未反序列化到内存），使用 table_store_cache_ 判断
      // 是否已含 major 和 minor sstable；同时要求 major 版本号大于 1，
      // 排除建表时自动生成的初始 DDL sstable。
      major_minor_ready = probe_handle.get_obj()->get_major_table_count() > 0
                          && probe_handle.get_obj()->get_minor_table_count() > 0
                          && probe_handle.get_obj()->get_last_major_snapshot_version() > 1;
    }
    if (!major_minor_ready) { ob_usleep(100 * 1000); }
  }
  ASSERT_TRUE(major_minor_ready);

  // 记录 checkpoint 点位的整表内容，恢复后要与之逐行一致。
  std::map<int64_t, int64_t> rows_at_ckpt;
  dump_rows(table_name.ptr(), rows_at_ckpt);
  ASSERT_EQ(18, rows_at_ckpt.size());

  // major merge 后本地 tablet 可能有多于 1 个 major sstable（DDL + merged），
  // acquire_first_created_ss_tablet_ 不支持 >1 major 的情况。
  // 但 inc upload 已经把最新版本写入 shared meta，直接获取即可；
  // 仅在 shared meta 中不存在时才走 clean + create 路径。
  {
    ObArenaAllocator try_alloc("CkptTryGet");
    ObTabletHandle try_handle;
    share::SCN try_meta_scn;
    if (OB_SUCCESS != meta_svr->get_tablet(
            ls_id, tablet_id, reorgan_scn, try_alloc, try_handle, try_meta_scn)) {
      CLEAN_SYS_TABLET_META(meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn);
      ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(
          ls_id, tablet_id, reorgan_scn, true/*check_lease*/));
    }
  }

  ObArenaAllocator shared_tablet_allocator("CkptE2EShared");
  ObTabletHandle shared_tablet_handle;
  share::SCN tablet_meta_scn;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(
      ls_id, tablet_id, reorgan_scn, shared_tablet_allocator,
      shared_tablet_handle, tablet_meta_scn));
  ASSERT_TRUE(shared_tablet_handle.is_valid());
  ASSERT_TRUE(tablet_meta_scn.is_valid_and_not_min());

  ObSSMetaTabletKey tablet_key;
  tablet_key.ls_id_ = ls_id;
  tablet_key.tablet_id_ = tablet_id;
  tablet_key.reorganization_scn_ = reorgan_scn;
  tablet_key.meta_scn_ = tablet_meta_scn;

  // 只写当前 tablet，避免全量 SYS_LS checkpoint 对本用例引入
  // 无关 tablet 和 PALF_KV 依赖，但 tablet/sub-meta/checkpoint 都走真实写链路。
  ObSSLsCkptWriter ckpt_writer(true /*need_check_existing_object*/);
  ObSSLsCkptData ckpt_data;
  ASSERT_EQ(OB_SUCCESS, upload_tablet_meta_for_test(ckpt_writer, tablet_key, ckpt_data));
  ASSERT_EQ(1, ckpt_data.tablet_meta_keys_.count());
  ASSERT_EQ(OB_SUCCESS, ckpt_writer.collect_ls_meta(
      ls_id, tablet_meta_scn, ckpt_data.ls_meta_));
  ckpt_data.ckpt_scn_ = tablet_meta_scn;
  ckpt_data.ckpt_ts_ = ObTimeUtility::current_time();
  ckpt_data.reserved_meta_scn_ = tablet_meta_scn;
  ASSERT_TRUE(ckpt_data.is_valid());
  ASSERT_EQ(OB_SUCCESS, ckpt_writer.write_checkpoint_object(ls_id, ckpt_data));

  ObSSMetaLSCkptKey ckpt_key;
  ckpt_key.ls_id_ = ls_id;
  ckpt_key.meta_scn_ = ckpt_data.ckpt_scn_;
  const blocksstable::MacroBlockId ckpt_macro_id =
      ckpt_key.convert_to_macro_id(false/*is_in_table*/);
  ASSERT_TRUE(ckpt_macro_id.is_valid());
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_LS_CKPT,
            ckpt_macro_id.storage_object_type());
  ASSERT_TRUE(ckpt_macro_id.is_in_object_storage());

  ObSSLsCkptReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init());
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      reader.load_checkpoint(ObLSID(ObLSID::SYS_LS_ID + 1), ckpt_macro_id));
  ASSERT_EQ(OB_SUCCESS, reader.load_checkpoint(ls_id, ckpt_macro_id));
  ASSERT_TRUE(reader.is_loaded());
  ASSERT_EQ(ckpt_macro_id, reader.get_ckpt_macro_id());
  ASSERT_EQ(ls_id, reader.get_ls_meta().ls_id_);
  ASSERT_EQ(ckpt_data.ckpt_scn_, reader.get_ckpt_scn());
  ASSERT_EQ(ckpt_data.reserved_meta_scn_, reader.get_reserved_meta_scn());
  ASSERT_EQ(1, reader.get_tablet_meta_macro_ids().count());

  ObArenaAllocator reader_allocator("CkptE2EReader");
  ObTabletHandle restored_tablet_handle;
  ObSSUpdateTabletLog restored_update_log;
  ASSERT_EQ(OB_SUCCESS, reader.read_tablet(
      reader.get_tablet_meta_macro_ids().at(0), reader_allocator,
      restored_tablet_handle, restored_update_log));
  ASSERT_TRUE(restored_tablet_handle.is_valid());
  ASSERT_EQ(ls_id, restored_tablet_handle.get_obj()->get_ls_id());
  ASSERT_EQ(tablet_id, restored_tablet_handle.get_obj()->get_tablet_id());
  ASSERT_TRUE(restored_update_log.is_valid());
  ASSERT_EQ(ls_id, restored_update_log.ls_id_);
  ASSERT_EQ(tablet_id, restored_update_log.tablet_id_);
  ASSERT_EQ(reorgan_scn, restored_update_log.reorg_scn_);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);

  // SQL DDL 新建的本地 tablet 允许暂驻内存；本用例后面需要删除旧的
  // PRIVATE_TABLET_META 来模拟从 shared tablet 重建，因此先把前置状态固定为已落盘。
  ObTabletHandle persisted_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet_svr()->persist_if_in_memory(
      tablet_id, persisted_tablet_handle));
  persisted_tablet_handle.reset();

  ObTabletHandle original_local_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls->ha_get_tablet(tablet_id, original_local_tablet_handle));
  ASSERT_TRUE(original_local_tablet_handle.is_valid());
  ASSERT_TRUE(original_local_tablet_handle.get_obj()->get_tablet_addr().is_block());
  blocksstable::MacroBlockId original_private_tablet_meta_id;
  ASSERT_EQ(OB_SUCCESS,
      original_local_tablet_handle.get_obj()->get_tablet_addr().get_macro_block_id(
          original_private_tablet_meta_id));
  ASSERT_TRUE(original_private_tablet_meta_id.is_private());
  ASSERT_EQ(blocksstable::ObStorageObjectType::PRIVATE_TABLET_META,
            original_private_tablet_meta_id.storage_object_type());

  // do_remove_tablet 只移除本地映射和事务路由，不产生 WAIT_GC 记录；
  // 同时删除旧 PRIVATE_TABLET_META，避免重建时触发文件不允许覆盖。
  bool local_tablet_needs_restore = false;
  auto restore_local_tablet_guard = MAKE_SCOPE(
      local_tablet_needs_restore, [&](bool &needs_restore) {
        if (needs_restore) {
          const int tmp_ret = file_mgr->delete_file_with_ls_context(
              original_private_tablet_meta_id, ls->get_ls_epoch(), ls_id.id());
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN_RET(tmp_ret,
                         "failed to clean old private tablet meta before test recovery",
                         K(original_private_tablet_meta_id), K(ls_id));
          }
          ret = ls->create_or_update_with_ss_tablet(
              *restored_tablet_handle.get_obj(), tablet_meta_scn, share::SCN::min_scn());
          if (OB_SUCCESS != ret) {
            ADD_FAILURE() << "失败路径恢复本地私有 tablet 失败, ret=" << ret;
          }
        }
      });

  original_local_tablet_handle.reset();
  ret = ls->get_tablet_svr()->do_remove_tablet(ls_id, tablet_id);
  if (OB_SUCCESS == ret) {
    local_tablet_needs_restore = true;
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle missing_tablet_handle;
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ls->ha_get_tablet(tablet_id, missing_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, file_mgr->delete_file_with_ls_context(
      original_private_tablet_meta_id, ls->get_ls_epoch(), ls_id.id()));

  ret = ls->create_or_update_with_ss_tablet(
      *restored_tablet_handle.get_obj(), tablet_meta_scn, share::SCN::min_scn());
  if (OB_SUCCESS == ret) {
    local_tablet_needs_restore = false;
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle local_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls->ha_get_tablet(tablet_id, local_tablet_handle));
  ASSERT_TRUE(local_tablet_handle.is_valid());
  ASSERT_EQ(ls_id, local_tablet_handle.get_obj()->get_ls_id());
  ASSERT_EQ(tablet_id, local_tablet_handle.get_obj()->get_tablet_id());
  ASSERT_TRUE(local_tablet_handle.get_obj()->get_tablet_addr().is_block());
  blocksstable::MacroBlockId restored_private_tablet_meta_id;
  ASSERT_EQ(OB_SUCCESS,
      local_tablet_handle.get_obj()->get_tablet_addr().get_macro_block_id(
          restored_private_tablet_meta_id));
  ASSERT_TRUE(restored_private_tablet_meta_id.is_private());
  ASSERT_EQ(blocksstable::ObStorageObjectType::PRIVATE_TABLET_META,
            restored_private_tablet_meta_id.storage_object_type());

  // ---------- 步骤 2：恢复后先校验数据与 checkpoint 点位一致 ----------
  // 不做任何写入，直接读整表，应与备份时的 rows_at_ckpt 完全相等。
  // 这一步证明恢复出的 tablet 真的能读到 checkpoint 里那批 major/minor 数据。
  std::map<int64_t, int64_t> rows_after_restore;
  dump_rows(table_name.ptr(), rows_after_restore);
  ASSERT_EQ(rows_at_ckpt, rows_after_restore);

  // ---------- 步骤 3：在恢复的 tablet 上继续写入，再校验数据 ----------
  // 新增 2 行 + 改 1 行，确认恢复出来的 tablet 事务路由和 memtable 都可写。
  ObSqlString ins_sql;
  ASSERT_EQ(OB_SUCCESS, ins_sql.assign_fmt(
      "insert into ss_meta_test.%s values (100, 1000), (101, 1010)",
      table_name.ptr()));
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(ins_sql.ptr()));
  ObSqlString upd_sql;
  ASSERT_EQ(OB_SUCCESS, upd_sql.assign_fmt(
      "update ss_meta_test.%s set b = 999 where a = 2", table_name.ptr()));
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(upd_sql.ptr()));

  // 期望 = 备份数据 + 这两处改动。
  std::map<int64_t, int64_t> expected = rows_at_ckpt;
  expected[100] = 1000;
  expected[101] = 1010;
  expected[2] = 999;
  std::map<int64_t, int64_t> rows_after_write;
  dump_rows(table_name.ptr(), rows_after_write);
  ASSERT_EQ(expected, rows_after_write);

  local_tablet_handle.reset();
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(drop_table_sql.ptr()));
  table_needs_drop = false;
  blocksstable::MacroBlockId invalid_ckpt_macro_id;
  ASSERT_EQ(OB_INVALID_DATA, reader.load_checkpoint(ls_id, invalid_ckpt_macro_id));
  ASSERT_FALSE(reader.is_loaded());
  ASSERT_FALSE(reader.get_ckpt_macro_id().is_valid());
  LOG_INFO("[TEST CASE END] sys_ls_ckpt_restores_working_private_tablet",
           K(ls_id), K(tablet_id), K(tablet_meta_scn),
           K(ckpt_macro_id), K(restored_private_tablet_meta_id));
}

/**
 * 概要：如果 tablet 声称自己引用了某个 sub-meta，但对象存储里其实没有，
 * 上传必须失败，而且不能把地址改到一半就留下脏状态。
 *
 * 覆盖模块：
 * 1. ObSSLsCkptWriter：上传完成后校验 tablet macro info 中的所有 sub-meta 引用。
 * 2. 地址转换的两阶段处理：所有对象都存在时才统一把 TABLE 地址改成 OSS 地址。
 *
 * 测试流程：
 * 1. 创建并持久化一个真实 SYS tablet，取得它当前的 macro info。
 * 2. 在 macro info 末尾补一个实际不存在的 sub-meta 引用，模拟上传漏对象。
 * 3. 调用 writer 的 tablet 上传与重写逻辑，确认返回 OB_OBJECT_NOT_EXIST。
 * 4. 再检查 tablet 和 macro info 的地址都没有变化，避免失败后留下半转换状态。
 */
TEST_F(TestSharedMetaService, sys_ls_ckpt_rejects_missing_sub_meta)
{
  LOG_INFO("[TEST CASE BEGIN] sys_ls_ckpt_rejects_missing_sub_meta");
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  INIT_SYS_TIERED_STORE_V2(tiered_store);

  const int64_t table_suffix = ObTimeUtility::current_time();
  ObSqlString table_name;
  ObSqlString create_table_sql;
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("create database if not exists ss_meta_test"));
  ASSERT_EQ(OB_SUCCESS, table_name.assign_fmt("sys_ls_ckpt_missing_ref_%ld", table_suffix));
  ASSERT_EQ(OB_SUCCESS, create_table_sql.assign_fmt(
      "create table ss_meta_test.%s (a int)", table_name.ptr()));
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(create_table_sql.ptr()));

  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_sys_tablet_and_ls_id(table_name.ptr(), tablet_id, ls_id));
  ASSERT_TRUE(tablet_id.is_valid());
  ASSERT_TRUE(ls_id.is_valid());

  const share::SCN reorgan_scn = share::SCN::min_scn();
  CLEAN_SYS_TABLET_META(meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn);
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(
      ls_id, tablet_id, reorgan_scn, true /*check_lease*/));

  ObArenaAllocator allocator("CkptMissingRef");
  ObTabletHandle tablet_handle;
  share::SCN tablet_meta_scn;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(
      ls_id, tablet_id, reorgan_scn, allocator, tablet_handle, tablet_meta_scn));
  ASSERT_TRUE(tablet_handle.is_valid());

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  ObTabletMacroInfo *macro_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, tablet->load_macro_info(0 /*ls_epoch*/, allocator, macro_info));
  ASSERT_NE(nullptr, macro_info);
  ASSERT_GT(macro_info->shared_meta_block_info_arr_.cnt_, 0);

  ObArray<blocksstable::MacroBlockId> original_ids;
  int64_t max_op_id = 0;
  for (int64_t i = 0; i < macro_info->shared_meta_block_info_arr_.cnt_; ++i) {
    const blocksstable::MacroBlockId &macro_id =
        macro_info->shared_meta_block_info_arr_.arr_[i];
    ASSERT_EQ(OB_SUCCESS, original_ids.push_back(macro_id));
    max_op_id = MAX(max_op_id, macro_id.op_id());
  }

  blocksstable::MacroBlockId missing_id = original_ids.at(0);
  missing_id.set_op_id(max_op_id + 1);
  ASSERT_TRUE(missing_id.is_shared_tablet_sub_meta_in_table());
  blocksstable::MacroBlockId missing_oss_id = missing_id;
  missing_oss_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
  bool missing_oss_exist = false;
  ASSERT_EQ(OB_SUCCESS, blocksstable::ObObjectManager::ss_is_exist_object(
      missing_oss_id, 0 /*ls_epoch*/, missing_oss_exist));
  ASSERT_FALSE(missing_oss_exist);

  const int64_t new_count = original_ids.count() + 1;
  blocksstable::MacroBlockId *expanded_ids = static_cast<blocksstable::MacroBlockId *>(
      allocator.alloc(sizeof(blocksstable::MacroBlockId) * new_count));
  ASSERT_NE(nullptr, expanded_ids);
  for (int64_t i = 0; i < original_ids.count(); ++i) {
    expanded_ids[i] = original_ids.at(i);
  }
  expanded_ids[new_count - 1] = missing_id;
  macro_info->shared_meta_block_info_arr_.arr_ = expanded_ids;
  macro_info->shared_meta_block_info_arr_.cnt_ = new_count;
  macro_info->shared_meta_block_info_arr_.capacity_ = new_count;

  blocksstable::MacroBlockId original_table_store_id;
  blocksstable::MacroBlockId original_storage_schema_id;
  ASSERT_EQ(OB_SUCCESS, tablet->table_store_addr_.addr_.get_macro_block_id(
      original_table_store_id));
  ASSERT_EQ(OB_SUCCESS, tablet->storage_schema_addr_.addr_.get_macro_block_id(
      original_storage_schema_id));

  uint64_t data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), data_version));
  ObSSMetaTabletKey tablet_key;
  tablet_key.ls_id_ = ls_id;
  tablet_key.tablet_id_ = tablet_id;
  tablet_key.reorganization_scn_ = reorgan_scn;
  tablet_key.meta_scn_ = tablet_meta_scn;
  ObSSLsCkptWriter writer(true /*need_check_existing_object*/);
  writer.init_write_tablet_context_(tablet_key, tiered_store, allocator);
  writer.tablet_ = tablet;
  writer.tablet_meta_.data_version_ = data_version;
  writer.macro_info_ = macro_info;
  ASSERT_EQ(OB_OBJECT_NOT_EXIST,
      writer.upload_sub_meta_and_redirect_tablet_());

  blocksstable::MacroBlockId current_table_store_id;
  blocksstable::MacroBlockId current_storage_schema_id;
  ASSERT_EQ(OB_SUCCESS, tablet->table_store_addr_.addr_.get_macro_block_id(
      current_table_store_id));
  ASSERT_EQ(OB_SUCCESS, tablet->storage_schema_addr_.addr_.get_macro_block_id(
      current_storage_schema_id));
  ASSERT_EQ(original_table_store_id, current_table_store_id);
  ASSERT_EQ(original_storage_schema_id, current_storage_schema_id);
  for (int64_t i = 0; i < original_ids.count(); ++i) {
    ASSERT_EQ(original_ids.at(i), macro_info->shared_meta_block_info_arr_.arr_[i]);
  }
  ASSERT_EQ(missing_id, macro_info->shared_meta_block_info_arr_.arr_[new_count - 1]);
  LOG_INFO("[TEST CASE END] sys_ls_ckpt_rejects_missing_sub_meta",
           K(tablet_id), K(missing_id));
}

/**
 * 概要：LS checkpoint 必须拒绝持久化 payload 中标记为非纯行存的 SYS 用户 tablet。
 *
 * 共享存储模式已禁止通过正常 DDL 在 SYS tenant 创建列存表，因此本用例先创建
 * 合法行存 tablet，再复制其持久化 meta 作为新版本，并仅在副本中注入非行存标记。
 *
 * 测试流程：
 * 1. 在 SYS tenant 创建普通行存用户表，并生成合法 shared tablet meta。
 * 2. 复制持久化 tablet meta 为新版本，将副本的 tablet cache 标记为非纯行存。
 * 3. 调用单 tablet checkpoint 上传入口，确认返回 OB_NOT_SUPPORTED 且 manifest 中没有该 tablet。
 * 4. 确认 tablet meta 和它引用的全部 sub-meta 都没有写入对象存储。
 */
TEST_F(TestSharedMetaService, sys_ls_ckpt_rejects_column_store_tablet)
{
  LOG_INFO("[TEST CASE BEGIN] sys_ls_ckpt_rejects_column_store_tablet");
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  INIT_SYS_TIERED_STORE_V2(tiered_store);

  const int64_t table_suffix = ObTimeUtility::current_time();
  ObSqlString table_name;
  ObSqlString create_table_sql;
  ObSqlString drop_table_sql;
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("create database if not exists ss_meta_test"));
  ASSERT_EQ(OB_SUCCESS, table_name.assign_fmt(
      "sys_ls_ckpt_reject_cs_%ld", table_suffix));
  ASSERT_EQ(OB_SUCCESS, create_table_sql.assign_fmt(
      "create table ss_meta_test.%s (a int primary key, b int)", table_name.ptr()));
  ASSERT_EQ(OB_SUCCESS, drop_table_sql.assign_fmt(
      "drop table if exists ss_meta_test.%s", table_name.ptr()));
  const share::SCN reorgan_scn = share::SCN::min_scn();
  SysLsCkptPauseGuard ckpt_pause_guard;
  ObTabletID tablet_id;
  ObLSID ls_id;
  bool ckpt_pause_attempted = false;
  auto cleanup_resources = [&]() {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    if (!ckpt_pause_attempted) {
      ckpt_pause_attempted = true;
      tmp_ret = ckpt_pause_guard.pause();
      if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("pause sys ls checkpoint before column store cleanup failed",
                 KR(tmp_ret), K(tablet_id), K(ls_id));
      }
    }
    tmp_ret = sys_exe_sql(drop_table_sql.ptr());
    if (OB_SUCCESS != tmp_ret) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
      LOG_WARN("drop column store checkpoint test table failed",
               KR(tmp_ret), K(tablet_id), K(ls_id));
    }
    if (tablet_id.is_valid() && ls_id.is_valid()) {
      tmp_ret = cleanup_sys_tablet_meta(
          *meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn);
      if (OB_SUCCESS != tmp_ret) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        LOG_WARN("cleanup column store checkpoint tablet meta failed",
                 KR(tmp_ret), K(tablet_id), K(ls_id));
      }
    }
    return ret;
  };
  bool resources_need_cleanup = true;
  auto resources_cleanup_guard = MAKE_SCOPE(
      resources_need_cleanup, [&](bool &needs_cleanup) {
        if (needs_cleanup) {
          const int tmp_ret = cleanup_resources();
          if (OB_SUCCESS != tmp_ret) {
            ADD_FAILURE() << "清理列存 checkpoint 测试资源失败, ret=" << tmp_ret;
          }
        }
      });

  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(create_table_sql.ptr()));
  ASSERT_EQ(OB_SUCCESS, get_sys_tablet_and_ls_id(table_name.ptr(), tablet_id, ls_id));
  ASSERT_TRUE(tablet_id.is_valid());
  ASSERT_FALSE(tablet_id.is_inner_tablet());
  ASSERT_TRUE(ls_id.is_sys_ls());

  ASSERT_EQ(OB_SUCCESS, cleanup_sys_tablet_meta(
      *meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(
      ls_id, tablet_id, reorgan_scn, true /*check_lease*/));

  ObArenaAllocator allocator("CkptRejectCS");
  ObTabletHandle tablet_handle;
  share::SCN tablet_meta_scn;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(
      ls_id, tablet_id, reorgan_scn, allocator, tablet_handle, tablet_meta_scn));
  ASSERT_TRUE(tablet_handle.is_valid());
  ASSERT_TRUE(tablet_meta_scn.is_valid());
  ASSERT_TRUE(tablet_handle.get_obj()->is_row_store());

  ObTabletMacroInfo *macro_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->load_macro_info(
      0 /*ls_epoch*/, allocator, macro_info));
  ASSERT_NE(nullptr, macro_info);
  ObArray<blocksstable::MacroBlockId> referenced_ids;
  for (int64_t i = 0; i < macro_info->shared_meta_block_info_arr_.cnt_; ++i) {
    const blocksstable::MacroBlockId &macro_id =
        macro_info->shared_meta_block_info_arr_.arr_[i];
    if (macro_id.is_shared_tablet_sub_meta_in_table()) {
      ASSERT_EQ(OB_SUCCESS, referenced_ids.push_back(macro_id));
    }
  }
  ASSERT_GT(referenced_ids.count(), 0);

  ObSSMetaTabletKey source_tablet_key;
  source_tablet_key.ls_id_ = ls_id;
  source_tablet_key.tablet_id_ = tablet_id;
  source_tablet_key.reorganization_scn_ = reorgan_scn;
  source_tablet_key.meta_scn_ = tablet_meta_scn;
  ObSSMetaTabletRow source_tablet_row;
  ASSERT_EQ(OB_SUCCESS, meta_svr->read_tablet_meta(
      source_tablet_key, allocator, source_tablet_row));

  ObSSLsCkptWriter meta_builder(true /*need_check_existing_object*/);
  meta_builder.init_write_tablet_context_(
      source_tablet_key, tiered_store, allocator);
  ASSERT_EQ(OB_SUCCESS, meta_builder.build_tablet_meta_(source_tablet_row.value_));
  ASSERT_NE(nullptr, meta_builder.tablet_);
  ASSERT_TRUE(meta_builder.tablet_->is_row_store());
  meta_builder.tablet_->table_store_cache_.is_row_store_ = false;
  ASSERT_FALSE(meta_builder.tablet_->is_row_store());
  ObString non_row_store_tablet_value;
  ASSERT_EQ(OB_SUCCESS, meta_builder.serialize_tablet_meta_(
      non_row_store_tablet_value));

  ObSSMetaTabletKey tablet_key(source_tablet_key);
  tablet_key.meta_scn_ = share::SCN::plus(tablet_meta_scn, 1);
  ASSERT_TRUE(tablet_key.is_valid());
  const ObSSMetaTabletRow non_row_store_tablet_row(
      static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META),
      tablet_key,
      non_row_store_tablet_value);
  ASSERT_EQ(OB_SUCCESS, meta_svr->insert_tablet_meta(
      non_row_store_tablet_row));

  ObSSMetaTabletKey tablet_key_copy(tablet_key);
  blocksstable::MacroBlockId tablet_oss_id =
      tablet_key_copy.convert_to_macro_id(true /*is_in_table*/);
  tablet_oss_id.set_location(
      blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
  ASSERT_TRUE(tablet_oss_id.is_valid());

  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, blocksstable::ObObjectManager::ss_is_exist_object(
      tablet_oss_id, 0 /*ls_epoch*/, is_exist));
  ASSERT_FALSE(is_exist);
  for (int64_t i = 0; i < referenced_ids.count(); ++i) {
    blocksstable::MacroBlockId obj_id = referenced_ids.at(i);
    obj_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
    ASSERT_EQ(OB_SUCCESS, blocksstable::ObObjectManager::ss_is_exist_object(
        obj_id, 0 /*ls_epoch*/, is_exist));
    ASSERT_FALSE(is_exist);
  }

  // 即使所有 tablet 都统一拒绝列存布局，也必须先拒绝 key 与 payload 身份错配。
  ObSSMetaTabletRow tablet_row;
  ASSERT_EQ(OB_SUCCESS, meta_svr->read_tablet_meta(
      tablet_key, allocator, tablet_row));
  ObSSMetaTabletKey mismatched_key(tablet_key);
  mismatched_key.tablet_id_ =
      ObTabletID(share::OB_ALL_TENANT_TIME_ZONE_TRANSITION_TID);
  ObSSLsCkptWriter writer(true /*need_check_existing_object*/);
  blocksstable::MacroBlockId mismatched_obj_id;
  ASSERT_EQ(OB_INVALID_DATA, writer.write_tablet_meta_(
      mismatched_key, tablet_row.value_, tiered_store, allocator, mismatched_obj_id));
  ASSERT_FALSE(mismatched_obj_id.is_valid());

  blocksstable::MacroBlockId unsupported_obj_id;
  ASSERT_EQ(OB_NOT_SUPPORTED, writer.write_tablet_meta_(
      tablet_key, tablet_row.value_, tiered_store, allocator, unsupported_obj_id));
  ASSERT_FALSE(unsupported_obj_id.is_valid());

  ObSSLsCkptData ckpt_data;
  ASSERT_EQ(OB_NOT_SUPPORTED, upload_tablet_meta_for_test(writer, tablet_key, ckpt_data));
  ASSERT_EQ(0, ckpt_data.tablet_meta_keys_.count());
  ASSERT_EQ(OB_SUCCESS, blocksstable::ObObjectManager::ss_is_exist_object(
      tablet_oss_id, 0 /*ls_epoch*/, is_exist));
  ASSERT_FALSE(is_exist);
  for (int64_t i = 0; i < referenced_ids.count(); ++i) {
    blocksstable::MacroBlockId obj_id = referenced_ids.at(i);
    obj_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
    ASSERT_EQ(OB_SUCCESS, blocksstable::ObObjectManager::ss_is_exist_object(
        obj_id, 0 /*ls_epoch*/, is_exist));
    ASSERT_FALSE(is_exist);
  }

  tablet_handle.reset();
  ASSERT_EQ(OB_SUCCESS, cleanup_resources());
  resources_need_cleanup = false;
  LOG_INFO("[TEST CASE END] sys_ls_ckpt_rejects_column_store_tablet",
           K(tablet_key), K(referenced_ids.count()));
}

/**
 * 概要：table store 中只要存在 CO/CG SSTable，就必须在任何对象上传前被识别。
 *
 * 覆盖模块：
 * 1. ObSSLsCkptWriter 的 no-load table store 预检。
 * 2. 用户 tablet 返回 OB_NOT_SUPPORTED 的拒绝策略。
 * 3. SSTable 上传入口的防御性检查，确保 CO/CG 不会绕过预检进入上传路径。
 *
 * 测试流程：
 * 1. 手工构造一个 CO major SSTable，并在其中挂一个 CG SSTable。
 * 2. 确认 table store 预检能够识别列存 SSTable。
 * 3. 验证用户 tablet 返回 OB_NOT_SUPPORTED。
 * 4. 对缺失的 table store、storage schema、SSTable meta 行分别断言
 *    OB_OBJECT_NOT_EXIST，覆盖三个迭代器协议码转换入口。
 * 5. 直接调用列存 SSTable 上传入口，确认返回 OB_ERR_UNEXPECTED 且地址没有改写。
 * 6. 普通 SSTable 携带 OSS sub-meta block 地址时，确认其被当作非预期输入拒绝。
 */
TEST_F(TestSharedMetaService, sys_ls_ckpt_rejects_co_cg_before_upload)
{
  LOG_INFO("[TEST CASE BEGIN] sys_ls_ckpt_rejects_co_cg_before_upload");
  // Writer 依赖 SYS tenant 的 tiered meta 环境，但本用例不会写入任何对象。
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  INIT_SYS_TIERED_STORE_V2(tiered_store);

  const int64_t table_suffix = ObTimeUtility::current_time();
  ObSqlString table_name;
  ObSqlString create_table_sql;
  ObSqlString drop_table_sql;
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("create database if not exists ss_meta_test"));
  ASSERT_EQ(OB_SUCCESS, table_name.assign_fmt("sys_ls_ckpt_cg_%ld", table_suffix));
  ASSERT_EQ(OB_SUCCESS, create_table_sql.assign_fmt(
      "create table ss_meta_test.%s (a int)", table_name.ptr()));
  ASSERT_EQ(OB_SUCCESS, drop_table_sql.assign_fmt(
      "drop table if exists ss_meta_test.%s", table_name.ptr()));
  const share::SCN reorgan_scn = share::SCN::min_scn();
  SysLsCkptPauseGuard ckpt_pause_guard;
  ObTabletID tablet_id;
  ObLSID ls_id;
  bool ckpt_pause_attempted = false;
  auto cleanup_resources = [&]() {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    if (!ckpt_pause_attempted) {
      ckpt_pause_attempted = true;
      tmp_ret = ckpt_pause_guard.pause();
      if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("pause sys ls checkpoint before CO/CG cleanup failed",
                 KR(tmp_ret), K(tablet_id), K(ls_id));
      }
    }
    tmp_ret = sys_exe_sql(drop_table_sql.ptr());
    if (OB_SUCCESS != tmp_ret) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
      LOG_WARN("drop CO/CG checkpoint test table failed",
               KR(tmp_ret), K(tablet_id), K(ls_id));
    }
    if (tablet_id.is_valid() && ls_id.is_valid()) {
      tmp_ret = cleanup_sys_tablet_meta(
          *meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn);
      if (OB_SUCCESS != tmp_ret) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        LOG_WARN("cleanup CO/CG checkpoint tablet meta failed",
                 KR(tmp_ret), K(tablet_id), K(ls_id));
      }
    }
    return ret;
  };
  bool resources_need_cleanup = true;
  auto resources_cleanup_guard = MAKE_SCOPE(
      resources_need_cleanup, [&](bool &needs_cleanup) {
        if (needs_cleanup) {
          const int tmp_ret = cleanup_resources();
          if (OB_SUCCESS != tmp_ret) {
            ADD_FAILURE() << "清理 CO/CG checkpoint 测试资源失败, ret=" << tmp_ret;
          }
        }
      });

  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(create_table_sql.ptr()));
  ASSERT_EQ(OB_SUCCESS, get_sys_tablet_and_ls_id(table_name.ptr(), tablet_id, ls_id));
  ASSERT_TRUE(tablet_id.is_valid());
  ASSERT_TRUE(ls_id.is_valid());

  // 清理旧版本，避免本次构造的 tablet 被历史数据短路掉。
  ASSERT_EQ(OB_SUCCESS, cleanup_sys_tablet_meta(
      *meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(
      ls_id, tablet_id, reorgan_scn, true /*check_lease*/));

  ObArenaAllocator allocator("CkptColumn");
  ObTabletHandle row_tablet_handle;
  share::SCN tablet_meta_scn;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(
      ls_id, tablet_id, reorgan_scn, allocator,
      row_tablet_handle, tablet_meta_scn));
  ASSERT_TRUE(row_tablet_handle.is_valid());
  ASSERT_TRUE(row_tablet_handle.get_obj()->is_row_store());
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(
      allocator, table_schema, lib::Worker::CompatMode::MYSQL));

  const int64_t snapshot_version = 100;
  auto create_empty_major = [&](const ObITable::TableType table_type,
                                const int64_t column_group_count,
                                ObTableHandleV2 &table_handle) {
    int ret = OB_SUCCESS;
    ObITable::TableKey table_key;
    ObTabletCreateSSTableParam param;
    table_key.tablet_id_ = tablet_id;
    table_key.table_type_ = table_type;
    table_key.version_range_.base_version_ = 0;
    table_key.version_range_.snapshot_version_ = snapshot_version;
    if (OB_FAIL(param.init_for_empty_major_sstable(
            tablet_id, storage_schema, snapshot_version,
            -1 /*schema_version*/, false /*is_tmp_sstable*/, false /*is_shared_storage*/))) {
      LOG_WARN("failed to init empty major param", KR(ret), K(table_type));
    } else {
      param.table_key_ = table_key;
      param.column_group_cnt_ = column_group_count;
      if (ObITable::COLUMN_ORIENTED_SSTABLE == table_type) {
        param.co_base_type_ = ObCOSSTableBaseType::ALL_CG_TYPE;
        if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(
                param, allocator, table_handle))) {
          LOG_WARN("failed to create co sstable", KR(ret), K(param));
        }
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(
                     param, allocator, table_handle))) {
        LOG_WARN("failed to create cg sstable", KR(ret), K(param));
      }
    }
    return ret;
  };

  ObTableHandleV2 co_handle;
  ObTableHandleV2 cg_handle;
  ObTableHandleV2 row_handle;
  ASSERT_EQ(OB_SUCCESS, create_empty_major(
      ObITable::COLUMN_ORIENTED_SSTABLE, 2, co_handle));
  ASSERT_EQ(OB_SUCCESS, create_empty_major(
      ObITable::NORMAL_COLUMN_GROUP_SSTABLE, 1, cg_handle));
  ASSERT_EQ(OB_SUCCESS, create_empty_major(
      ObITable::MAJOR_SSTABLE, 1, row_handle));

  blocksstable::ObSSTable *co_base = nullptr;
  blocksstable::ObSSTable *cg_sstable = nullptr;
  blocksstable::ObSSTable *row_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, co_handle.get_sstable(co_base));
  ASSERT_EQ(OB_SUCCESS, cg_handle.get_sstable(cg_sstable));
  ASSERT_EQ(OB_SUCCESS, row_handle.get_sstable(row_sstable));
  ASSERT_NE(nullptr, co_base);
  ASSERT_NE(nullptr, cg_sstable);
  ASSERT_NE(nullptr, row_sstable);
  ASSERT_TRUE(co_base->is_co_sstable());
  ASSERT_TRUE(cg_sstable->is_cg_sstable());
  ASSERT_FALSE(ObITable::is_column_store_sstable(row_sstable->get_table_type()));
  ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(co_base);

  auto build_in_table_addr = [&](const int64_t data_seq,
                                 ObMetaDiskAddr &addr,
                                 blocksstable::MacroBlockId &macro_id) {
    ObSSMetaTieredMetaKey meta_key;
    meta_key.ls_id_ = ls_id;
    meta_key.tablet_id_ = tablet_id;
    meta_key.op_id_ = 987654;
    meta_key.data_seq_ = data_seq;
    meta_key.reorgan_scn_ = share::SCN::min_scn();
    macro_id = meta_key.convert_to_macro_id(true /*is_in_table*/);
    return addr.set_block_addr(
        macro_id, 0 /*offset*/, 4096 /*size*/, ObMetaDiskAddr::DiskType::RAW_BLOCK);
  };

  ObMetaDiskAddr root_addr;
  ObMetaDiskAddr cg_addr;
  ObMetaDiskAddr missing_addr;
  blocksstable::MacroBlockId root_in_table_id;
  blocksstable::MacroBlockId cg_in_table_id;
  blocksstable::MacroBlockId missing_in_table_id;
  ASSERT_EQ(OB_SUCCESS, build_in_table_addr(0, root_addr, root_in_table_id));
  ASSERT_EQ(OB_SUCCESS, build_in_table_addr(1, cg_addr, cg_in_table_id));
  ASSERT_EQ(OB_SUCCESS, build_in_table_addr(2, missing_addr, missing_in_table_id));
  ASSERT_TRUE(root_in_table_id.is_shared_tablet_sub_meta_in_table());
  ASSERT_TRUE(cg_in_table_id.is_shared_tablet_sub_meta_in_table());
  ASSERT_TRUE(missing_in_table_id.is_shared_tablet_sub_meta_in_table());
  // 先让 CG 指向表内 sub-meta，再把 CG 挂到 CO 里，模拟真实列存结构。
  ASSERT_EQ(OB_SUCCESS, cg_sstable->set_addr(cg_addr));
  ASSERT_EQ(OB_SUCCESS, row_sstable->set_addr(missing_addr));

  ObSEArray<ObITable *, 1> cg_tables;
  ASSERT_EQ(OB_SUCCESS, cg_tables.push_back(cg_sstable));
  ASSERT_EQ(OB_SUCCESS, co_sstable->fill_cg_sstables(cg_tables));

  // table store 初始化会深拷 CO root，后续预检与上传防线都针对持久化副本。
  ObTabletTableStore table_store;
  ObArray<ObITable *> sstables;
  ObArray<ObMetaDiskAddr> sstable_addrs;
  ObMajorChecksumInfo major_ckm_info;
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(co_sstable));
  ASSERT_EQ(OB_SUCCESS, sstable_addrs.push_back(root_addr));
  ASSERT_EQ(OB_SUCCESS, table_store.init(
      allocator, sstables, sstable_addrs, major_ckm_info));

  uint64_t data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), data_version));
  const int64_t table_store_size = table_store.get_serialize_size(data_version);
  ASSERT_GT(table_store_size, 0);
  char *table_store_buf = static_cast<char *>(allocator.alloc(table_store_size));
  ASSERT_NE(nullptr, table_store_buf);
  int64_t table_store_pos = 0;
  ASSERT_EQ(OB_SUCCESS, table_store.serialize(
      data_version, table_store_buf, table_store_size, table_store_pos));
  ASSERT_EQ(table_store_size, table_store_pos);
  const ObString table_store_value(
      static_cast<int32_t>(table_store_pos), table_store_buf);
  ASSERT_EQ(OB_SUCCESS, tiered_store.write_meta(
      root_in_table_id,
      static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE),
      table_store_value));
  bool table_store_meta_needs_cleanup = true;
  auto table_store_meta_guard = MAKE_SCOPE(
      table_store_meta_needs_cleanup, [&](bool &needs_cleanup) {
        if (needs_cleanup) {
          const int tmp_ret = tiered_store.del_meta(root_in_table_id);
          if (OB_SUCCESS != tmp_ret && OB_ENTRY_NOT_EXIST != tmp_ret) {
            ADD_FAILURE() << "清理测试构造的 CO table store 失败, ret=" << tmp_ret;
          }
        }
      });

  blocksstable::MacroBlockId root_oss_id = root_in_table_id;
  root_oss_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
  bool root_oss_exist = false;
  ASSERT_EQ(OB_SUCCESS, blocksstable::ObObjectManager::ss_is_exist_object(
      root_oss_id, 0 /*ls_epoch*/, root_oss_exist));
  ASSERT_FALSE(root_oss_exist);

  ObSSMetaTabletKey user_tablet_key;
  user_tablet_key.ls_id_ = ls_id;
  user_tablet_key.tablet_id_ = tablet_id;
  user_tablet_key.reorganization_scn_ = share::SCN::min_scn();
  user_tablet_key.meta_scn_ = tablet_meta_scn;
  ASSERT_TRUE(user_tablet_key.is_valid());
  ASSERT_FALSE(user_tablet_key.tablet_id_.is_inner_tablet());

  ObSSLsCkptWriter writer(true /*need_check_existing_object*/);
  writer.init_write_tablet_context_(user_tablet_key, tiered_store, allocator);
  writer.tablet_ = row_tablet_handle.get_obj();
  writer.tablet_meta_.data_version_ = data_version;

  // Empty private SSTables do not reference data macro blocks and are safe.
  // Once the same SSTable becomes non-empty, it must not pass the checkpoint
  // precheck until its macro-block ownership is marked shared.
  ObTabletTableStore row_table_store;
  ObArray<ObITable *> row_sstables;
  ObArray<ObMetaDiskAddr> row_sstable_addrs;
  ASSERT_EQ(OB_SUCCESS, row_sstables.push_back(row_sstable));
  ASSERT_EQ(OB_SUCCESS, row_sstable_addrs.push_back(missing_addr));
  ASSERT_EQ(OB_SUCCESS, row_table_store.init(
      allocator, row_sstables, row_sstable_addrs, major_ckm_info));
  ASSERT_EQ(OB_SUCCESS, writer.check_table_store_sstables_(row_table_store));

  const ObSSTableArray &row_major_sstables = row_table_store.get_major_sstables();
  ASSERT_EQ(1, row_major_sstables.count());
  blocksstable::ObSSTable *private_sstable = row_major_sstables.at(0);
  ASSERT_NE(nullptr, private_sstable);
  ASSERT_TRUE(private_sstable->is_empty());
  private_sstable->meta_cache_.data_macro_block_count_ = 1;
  private_sstable->meta_->get_basic_meta().data_macro_block_count_ = 1;
  ASSERT_FALSE(private_sstable->is_empty());
  blocksstable::ObSSTableMetaHandle private_meta_handle;
  ASSERT_EQ(OB_SUCCESS, private_sstable->get_meta(private_meta_handle));
  ASSERT_FALSE(private_meta_handle.get_sstable_meta()
      .get_table_shared_flag().is_shared_macro_blocks());
  ASSERT_EQ(OB_EAGAIN, writer.check_table_store_sstables_(row_table_store));
  private_sstable->meta_->get_basic_meta()
      .table_shared_flag_.set_share_macro_blocks();
  ASSERT_EQ(OB_SUCCESS, writer.check_table_store_sstables_(row_table_store));

  ASSERT_EQ(OB_NOT_SUPPORTED,
      writer.check_table_store_sstables_(table_store));
  ASSERT_TRUE(ObITable::is_column_store_sstable(co_sstable->get_table_type()));
  ASSERT_TRUE(ObITable::is_column_store_sstable(cg_sstable->get_table_type()));

  // 精确行缺失时，三个入口都必须把 OB_ITER_END 转换成业务错误码。
  ASSERT_EQ(OB_OBJECT_NOT_EXIST,
      writer.upload_table_store_(missing_in_table_id));
  ASSERT_EQ(OB_OBJECT_NOT_EXIST,
      writer.upload_tiered_meta_(missing_in_table_id));
  ASSERT_EQ(OB_OBJECT_NOT_EXIST,
      writer.upload_and_redirect_sstable_(*row_sstable));
  blocksstable::MacroBlockId missing_id_after;
  ASSERT_EQ(OB_SUCCESS,
      row_sstable->get_addr().get_macro_block_id(missing_id_after));
  ASSERT_EQ(missing_in_table_id, missing_id_after);

  // 当前持久化链路写入 table store 的 SSTable sub-meta 地址应位于表内；
  // OSS 地址或其他地址均属于非预期输入，且失败时不得改写地址。
  blocksstable::MacroBlockId unexpected_oss_id = missing_in_table_id;
  unexpected_oss_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
  ASSERT_TRUE(unexpected_oss_id.is_shared_tablet_sub_meta());
  ObMetaDiskAddr unexpected_oss_addr;
  ASSERT_EQ(OB_SUCCESS, unexpected_oss_addr.set_block_addr(
      unexpected_oss_id, 0 /*offset*/, 4096 /*size*/, ObMetaDiskAddr::DiskType::RAW_BLOCK));
  ASSERT_EQ(OB_SUCCESS, row_sstable->set_addr(unexpected_oss_addr));
  const ObMetaDiskAddr unexpected_oss_addr_before = row_sstable->get_addr();
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      writer.upload_and_redirect_sstable_(*row_sstable));
  ASSERT_TRUE(unexpected_oss_addr_before == row_sstable->get_addr());
  ASSERT_EQ(OB_SUCCESS, row_sstable->set_addr(missing_addr));

  ASSERT_EQ(OB_NOT_SUPPORTED, writer.upload_table_store_(root_in_table_id));
  ASSERT_EQ(OB_SUCCESS, blocksstable::ObObjectManager::ss_is_exist_object(
      root_oss_id, 0 /*ls_epoch*/, root_oss_exist));
  ASSERT_FALSE(root_oss_exist);

  const ObSSTableArray &major_sstables = table_store.get_major_sstables();
  ASSERT_EQ(1, major_sstables.count());
  blocksstable::MacroBlockId root_id_before;
  ASSERT_EQ(OB_SUCCESS, major_sstables.at(0)->get_addr().get_macro_block_id(
      root_id_before));
  ASSERT_EQ(root_in_table_id, root_id_before);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      writer.upload_and_redirect_sstables_(major_sstables));
  blocksstable::MacroBlockId root_id_after;
  ASSERT_EQ(OB_SUCCESS, major_sstables.at(0)->get_addr().get_macro_block_id(
      root_id_after));
  ASSERT_EQ(root_id_before, root_id_after);

  ASSERT_EQ(OB_SUCCESS, tiered_store.del_meta(root_in_table_id));
  table_store_meta_needs_cleanup = false;
  row_tablet_handle.reset();
  ASSERT_EQ(OB_SUCCESS, cleanup_resources());
  resources_need_cleanup = false;
  LOG_INFO("[TEST CASE END] sys_ls_ckpt_rejects_co_cg_before_upload",
           K(root_id_before), K(cg_in_table_id));
}

/**
 * 概要：验证 PALF KV 中的 checkpoint 指针只有完整反序列化且字段合法时才能使用。
 *
 * 覆盖模块：
 * 1. ObSSLsCkptFinishTask::parse_palf_kv_meta_scn_：反序列化对象 ID 并提取 SCN。
 * 2. validate_ls_ckpt_object_id：检查对象类型、存储位置、LS 和保留字段。
 *
 * 测试流程：
 * 1. 构造一个合法对象 ID，确认可以解析出预期 SCN。
 * 2. 依次改坏对象类型、存储位置、LS、保留字段和 SCN，确认都会被拒绝。
 * 3. 再传入乱码、空值和带多余字节的数据，确认返回 OB_INVALID_DATA。
 */
TEST(ObSSLsCkptDagTest, parse_palf_kv_rejects_invalid_pointer)
{
  // Start with a valid pointer and copy it for each invalid case,
  // changing only one field at a time.
  SCN ckpt_scn;
  ASSERT_EQ(OB_SUCCESS, ckpt_scn.convert_for_inner_table_field(100));

  ObSSMetaLSCkptKey ckpt_key;
  ckpt_key.ls_id_ = SYS_LS;
  ckpt_key.meta_scn_ = ckpt_scn;
  const blocksstable::MacroBlockId valid_id =
      ckpt_key.convert_to_macro_id(false /*is_in_table*/);
  ASSERT_TRUE(valid_id.is_valid());

  auto parse_id = [&](const blocksstable::MacroBlockId &macro_id,
                      SCN &meta_scn) {
    int ret = OB_SUCCESS;
    char value_buf[64] = {0};
    int64_t value_len = 0;
    if (OB_FAIL(macro_id.serialize(value_buf, sizeof(value_buf), value_len))) {
    } else {
      const ObString value(static_cast<int32_t>(value_len), value_buf);
      ret = ObSSLsCkptFinishTask::parse_palf_kv_meta_scn_(
          SYS_LS, value, meta_scn);
    }
    return ret;
  };

  // Verify that the valid PALF KV value yields the original checkpoint SCN.
  SCN meta_scn;
  ASSERT_EQ(OB_SUCCESS, parse_id(valid_id, meta_scn));
  ASSERT_EQ(ckpt_scn, meta_scn);

  // Corrupt one object ID field at a time to check each validation rule.
  blocksstable::MacroBlockId wrong_object_type(valid_id);
  wrong_object_type.set_storage_object_type(
      static_cast<uint64_t>(blocksstable::ObStorageObjectType::SHARED_LS_META));
  ASSERT_EQ(OB_INVALID_DATA, parse_id(wrong_object_type, meta_scn));

  blocksstable::MacroBlockId wrong_location(valid_id);
  wrong_location.set_location(blocksstable::ObMacroBlockIdLocation::TABLE);
  ASSERT_EQ(OB_INVALID_DATA, parse_id(wrong_location, meta_scn));

  blocksstable::MacroBlockId wrong_ls(valid_id);
  wrong_ls.set_second_id(ObLSID::SYS_LS_ID + 1);
  ASSERT_EQ(OB_INVALID_DATA, parse_id(wrong_ls, meta_scn));

  blocksstable::MacroBlockId wrong_third_id(valid_id);
  wrong_third_id.set_third_id(1);
  ASSERT_EQ(OB_INVALID_DATA, parse_id(wrong_third_id, meta_scn));

  blocksstable::MacroBlockId wrong_seq_id(valid_id);
  wrong_seq_id.set_seq_id(1);
  ASSERT_EQ(OB_INVALID_DATA, parse_id(wrong_seq_id, meta_scn));

  blocksstable::MacroBlockId invalid_scn(valid_id);
  invalid_scn.set_meta_scn(4611686018427387904LL);
  ASSERT_EQ(OB_INVALID_DATA, parse_id(invalid_scn, meta_scn));

  // Also reject malformed, empty, and trailing-data values.
  const ObString malformed_value = ObString::make_string("invalid-macro-id");
  ASSERT_EQ(OB_INVALID_DATA, ObSSLsCkptFinishTask::parse_palf_kv_meta_scn_(
      SYS_LS, malformed_value, meta_scn));
  ASSERT_EQ(OB_INVALID_DATA, ObSSLsCkptFinishTask::parse_palf_kv_meta_scn_(
      SYS_LS, ObString(), meta_scn));
  char trailing_buf[65] = {0};
  int64_t trailing_len = 0;
  ASSERT_EQ(OB_SUCCESS, valid_id.serialize(
      trailing_buf, sizeof(trailing_buf), trailing_len));
  trailing_buf[trailing_len++] = 'x';
  ASSERT_EQ(OB_INVALID_DATA, ObSSLsCkptFinishTask::parse_palf_kv_meta_scn_(
      SYS_LS, ObString(static_cast<int32_t>(trailing_len), trailing_buf),
      meta_scn));
}

/**
 * 概要：
 * 模拟发布 LS checkpoint 时，PALF_KV 中出现携带更高 SCN、但对象类型非法的
 * checkpoint 指针。确认指针解析失败后，失败清理只删除本轮新建的
 * LS_CKPT reservation，不会误删更早的历史 reservation。
 *
 * 覆盖模块：
 * 1. ObSSLsCkptFinishTask::parse_palf_kv_meta_scn_：拒绝非法的 PALF_KV
 *    checkpoint 指针。
 * 2. ObSSLsCkptFinishTask::remove_reserved_meta_scn_：验证失败路径只精确删除
 *    本轮 reservation。
 * 3. ObSharedMetaReservedOperator：准备、扫描并核对 LS_CKPT reservation。
 *
 * 测试流程：
 * 1. 构造历史 SCN、本轮 SCN 和更高的非法 SCN，并插入历史、本轮两条
 *    LS_CKPT reservation。
 * 2. 使用非法 SCN 构造对象类型错误的 PALF_KV MacroBlockId，序列化后确认
 *    parse_palf_kv_meta_scn_ 返回 OB_INVALID_DATA。
 * 3. 模拟 Finish 的失败分支，按本轮 SCN 执行精确清理。
 * 4. 扫描 reservation，确认历史记录仍然存在、本轮记录已经删除。
 */
TEST_F(TestSharedMetaService,
       palf_invalid_high_scn_failure_cleanup_keeps_historical_reservation)
{
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  const ObSharedMetaReservedType ls_ckpt_type(
      ObSharedMetaReservedTypeEnum::LS_CKPT);
  // 使用固定低 SCN，避免与后台 checkpoint 的活跃 reservation 重叠。
  static const uint64_t SCN_BASE = 100000;
  SCN historical_scn;
  SCN current_scn;
  SCN invalid_high_scn;
  ASSERT_EQ(OB_SUCCESS, historical_scn.convert_for_tx(SCN_BASE));
  ASSERT_EQ(OB_SUCCESS, current_scn.convert_for_tx(SCN_BASE + 1));
  ASSERT_EQ(OB_SUCCESS, invalid_high_scn.convert_for_tx(SCN_BASE + 100));

  ObSharedMetaReservedOperator reserved_operator;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.init(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      10L * 1000L * 1000L /*sql_timeout_us*/));
  const ObSharedMetaReservedInfo historical_info(
      OB_SYS_TENANT_ID, ls_ckpt_type, historical_scn);
  const ObSharedMetaReservedInfo current_info(
      OB_SYS_TENANT_ID, ls_ckpt_type, current_scn);
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.delete_row(
      historical_info, affected_rows));
  ASSERT_EQ(OB_SUCCESS, reserved_operator.delete_row(
      current_info, affected_rows));
  ASSERT_EQ(OB_SUCCESS, reserved_operator.insert_row(
      historical_info, affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, reserved_operator.insert_row(
      current_info, affected_rows));
  ASSERT_EQ(1, affected_rows);
  bool rows_need_cleanup = true;
  auto reserved_rows_guard = MAKE_SCOPE(
      rows_need_cleanup, [&](bool &need_cleanup) {
        if (need_cleanup) {
          int64_t cleanup_rows = 0;
          int tmp_ret = reserved_operator.delete_row(
              historical_info, cleanup_rows);
          if (OB_SUCCESS != tmp_ret) {
            ADD_FAILURE() << "清理历史 LS_CKPT reservation 失败, ret=" << tmp_ret;
          }
          tmp_ret = reserved_operator.delete_row(current_info, cleanup_rows);
          if (OB_SUCCESS != tmp_ret) {
            ADD_FAILURE() << "清理本轮 LS_CKPT reservation 失败, ret=" << tmp_ret;
          }
        }
      });

  ObSSMetaLSCkptKey invalid_key;
  invalid_key.ls_id_ = ls_id;
  invalid_key.meta_scn_ = invalid_high_scn;
  blocksstable::MacroBlockId invalid_high_id =
      invalid_key.convert_to_macro_id(false /*is_in_table*/);
  invalid_high_id.set_storage_object_type(
      static_cast<uint64_t>(blocksstable::ObStorageObjectType::SHARED_LS_META));
  char value_buf[64] = {0};
  int64_t value_len = 0;
  ASSERT_EQ(OB_SUCCESS, invalid_high_id.serialize(
      value_buf, sizeof(value_buf), value_len));
  SCN parsed_scn;
  ASSERT_EQ(OB_INVALID_DATA, ObSSLsCkptFinishTask::parse_palf_kv_meta_scn_(
      ls_id, ObString(static_cast<int32_t>(value_len), value_buf),
      parsed_scn));

  // 对应 Finish 的失败分支：只删除本次 reservation。
  ASSERT_EQ(OB_SUCCESS, ObSSLsCkptFinishTask::remove_reserved_meta_scn_(ls_id, current_scn));

  ObArray<ObSharedMetaReservedInfo> remaining_infos;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.scan_rows(
      OB_SYS_TENANT_ID, ls_ckpt_type, 0 /*limit_count*/, remaining_infos));
  bool found_historical = false;
  bool found_current = false;
  for (int64_t i = 0; i < remaining_infos.count(); ++i) {
    found_historical = found_historical
        || remaining_infos.at(i).meta_scn_ == historical_scn;
    found_current = found_current
        || remaining_infos.at(i).meta_scn_ == current_scn;
  }
  ASSERT_TRUE(found_historical);
  ASSERT_FALSE(found_current);
}

/**
 * 覆盖模块：ObSSLsCkptFinishTask::remove_reserved_meta_scns_at_or_before_ 的成功清理边界。
 * 测试流程：
 * 1. 写入历史、本轮和未来三个 LS_CKPT reservation。
 * 2. 以本轮 SCN 为上界执行清理。
 * 3. 扫描剩余记录，验证历史和本轮记录已删除，未来记录仍保留。
 */
TEST_F(TestSharedMetaService, ls_ckpt_success_cleanup_removes_scns_at_or_before)
{
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  const ObSharedMetaReservedType ls_ckpt_type(ObSharedMetaReservedTypeEnum::LS_CKPT);
  // 范围清理只覆盖固定低 SCN，不会命中后台 checkpoint 使用的运行时 GTS。
  static const uint64_t SCN_BASE = 1;
  SCN historical_scn;
  SCN current_scn;
  SCN future_scn;
  ASSERT_EQ(OB_SUCCESS, historical_scn.convert_for_tx(SCN_BASE));
  ASSERT_EQ(OB_SUCCESS, current_scn.convert_for_tx(SCN_BASE + 1));
  ASSERT_EQ(OB_SUCCESS, future_scn.convert_for_tx(SCN_BASE + 2));

  ObSharedMetaReservedOperator reserved_operator;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.init(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      10L * 1000L * 1000L /*sql_timeout_us*/));
  const ObSharedMetaReservedInfo historical_info(
      OB_SYS_TENANT_ID, ls_ckpt_type, historical_scn);
  const ObSharedMetaReservedInfo current_info(
      OB_SYS_TENANT_ID, ls_ckpt_type, current_scn);
  const ObSharedMetaReservedInfo future_info(
      OB_SYS_TENANT_ID, ls_ckpt_type, future_scn);
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.delete_row(historical_info, affected_rows));
  ASSERT_EQ(OB_SUCCESS, reserved_operator.delete_row(current_info, affected_rows));
  ASSERT_EQ(OB_SUCCESS, reserved_operator.delete_row(future_info, affected_rows));
  ASSERT_EQ(OB_SUCCESS, reserved_operator.insert_row(historical_info, affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, reserved_operator.insert_row(current_info, affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, reserved_operator.insert_row(future_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  bool rows_need_cleanup = true;
  auto reserved_rows_guard = MAKE_SCOPE(rows_need_cleanup, [&](bool &need_cleanup) {
    if (need_cleanup) {
      int64_t cleanup_rows = 0;
      int tmp_ret = reserved_operator.delete_row(historical_info, cleanup_rows);
      if (OB_SUCCESS != tmp_ret) {
        ADD_FAILURE() << "清理历史 LS_CKPT reservation 失败, ret=" << tmp_ret;
      }
      tmp_ret = reserved_operator.delete_row(current_info, cleanup_rows);
      if (OB_SUCCESS != tmp_ret) {
        ADD_FAILURE() << "清理本轮 LS_CKPT reservation 失败, ret=" << tmp_ret;
      }
      tmp_ret = reserved_operator.delete_row(future_info, cleanup_rows);
      if (OB_SUCCESS != tmp_ret) {
        ADD_FAILURE() << "清理未来 LS_CKPT reservation 失败, ret=" << tmp_ret;
      }
    }
  });

  ASSERT_EQ(OB_SUCCESS,
      ObSSLsCkptFinishTask::remove_reserved_meta_scns_at_or_before_(SYS_LS, current_scn));

  ObArray<ObSharedMetaReservedInfo> remaining_infos;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.scan_rows(
      OB_SYS_TENANT_ID, ls_ckpt_type, 0 /*limit_count*/, remaining_infos));
  bool found_historical = false;
  bool found_current = false;
  bool found_future = false;
  for (int64_t i = 0; i < remaining_infos.count(); ++i) {
    found_historical = found_historical
        || remaining_infos.at(i).meta_scn_ == historical_scn;
    found_current = found_current
        || remaining_infos.at(i).meta_scn_ == current_scn;
    found_future = found_future
        || remaining_infos.at(i).meta_scn_ == future_scn;
  }
  ASSERT_FALSE(found_historical);
  ASSERT_FALSE(found_current);
  ASSERT_TRUE(found_future);
}

/**
 * 覆盖模块：ObSSLsCkptFinishTask::remove_reserved_meta_scns_at_or_before_ 的限量清理。
 *
 * 单轮扫描上限为 1024 行（与 ob_ss_ls_ckpt_dag.cpp 中
 * scan_limit_count 保持一致），残留超过上限时需要多轮清理。
 *
 * 测试流程：
 * 1. 批量写入 1026 条不超过上界的 LS_CKPT reservation，以及 1 条超出上界的
 *    未来 reservation。
 * 2. 第一轮清理后，验证按 meta_scn 递增只删除了最小的 1024 条，剩余 2 条
 *    最大的残留和未来记录仍保留。
 * 3. 第二轮清理后，验证残留全部删除，未来记录仍保留。
 */
TEST_F(TestSharedMetaService, ls_ckpt_success_cleanup_removes_residue_beyond_scan_limit)
{
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  static const int64_t SCAN_LIMIT_COUNT = 1024;
  static const int64_t RESIDUE_ROW_COUNT = SCAN_LIMIT_COUNT + 2;
  // 测试专属的固定低 SCN 区间，范围删除不会覆盖后台活跃 reservation。
  static const uint64_t SCN_BASE = 1000;
  const ObSharedMetaReservedType ls_ckpt_type(ObSharedMetaReservedTypeEnum::LS_CKPT);
  SCN upper_bound_scn;
  SCN future_scn;
  ASSERT_EQ(OB_SUCCESS, upper_bound_scn.convert_for_tx(
      SCN_BASE + RESIDUE_ROW_COUNT - 1));
  ASSERT_EQ(OB_SUCCESS, future_scn.convert_for_tx(SCN_BASE + RESIDUE_ROW_COUNT));

  ObSharedMetaReservedOperator reserved_operator;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.init(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      10L * 1000L * 1000L /*sql_timeout_us*/));

  auto count_residue_rows = [&](int64_t &residue_count, int64_t &min_residue_val,
                                bool &found_future) {
    int ret = OB_SUCCESS;
    residue_count = 0;
    min_residue_val = INT64_MAX;
    found_future = false;
    ObArray<ObSharedMetaReservedInfo> scan_infos;
    if (OB_FAIL(reserved_operator.scan_rows(
            OB_SYS_TENANT_ID, ls_ckpt_type, 0 /*limit_count*/, scan_infos))) {
      LOG_WARN("scan ls ckpt reserved rows failed", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < scan_infos.count(); ++i) {
        const int64_t meta_scn_val = static_cast<int64_t>(
            scan_infos.at(i).meta_scn_.get_val_for_tx());
        if (meta_scn_val >= static_cast<int64_t>(SCN_BASE)
            && meta_scn_val <= static_cast<int64_t>(upper_bound_scn.get_val_for_tx())) {
          ++residue_count;
          min_residue_val = meta_scn_val < min_residue_val ? meta_scn_val : min_residue_val;
        } else if (meta_scn_val == static_cast<int64_t>(future_scn.get_val_for_tx())) {
          found_future = true;
        }
      }
    }
    return ret;
  };

  bool rows_need_cleanup = true;
  auto reserved_rows_guard = MAKE_SCOPE(rows_need_cleanup, [&](bool &need_cleanup) {
    if (need_cleanup) {
      ObSqlString cleanup_sql;
      int tmp_ret = cleanup_sql.assign_fmt(
          "DELETE FROM oceanbase.%s WHERE tenant_id = %lu AND type = '%s' "
          "AND meta_scn >= %ld AND meta_scn <= %ld",
          share::OB_ALL_SHARED_META_RESERVED_SCN_TNAME,
          OB_SYS_TENANT_ID, ls_ckpt_type.to_str(),
          static_cast<int64_t>(SCN_BASE), future_scn.get_val_for_tx());
      if (OB_SUCCESS != tmp_ret) {
        ADD_FAILURE() << "构造 LS_CKPT reservation 清理 SQL 失败, ret=" << tmp_ret;
      } else if (OB_SUCCESS != (tmp_ret = sys_exe_sql(cleanup_sql.ptr()))) {
        ADD_FAILURE() << "清理 LS_CKPT reservation 失败, ret=" << tmp_ret;
      }
    }
  });

  // 清理本测试及前一条边界用例使用的固定低区间，避免异常退出残留占用扫描配额。
  {
    ObSqlString reset_sql;
    ASSERT_EQ(OB_SUCCESS, reset_sql.assign_fmt(
        "DELETE FROM oceanbase.%s WHERE tenant_id = %lu AND type = '%s' "
        "AND meta_scn >= %ld AND meta_scn <= %ld",
        share::OB_ALL_SHARED_META_RESERVED_SCN_TNAME,
        OB_SYS_TENANT_ID, ls_ckpt_type.to_str(),
        1L, future_scn.get_val_for_tx()));
    ASSERT_EQ(OB_SUCCESS, sys_exe_sql(reset_sql.ptr()));
  }

  // 单条 SQL 批量插入，避免逐行 insert 的往返开销。
  ObSqlString insert_sql;
  ASSERT_EQ(OB_SUCCESS, insert_sql.assign_fmt(
      "INSERT INTO oceanbase.%s (tenant_id, type, meta_scn, owner) VALUES ",
      share::OB_ALL_SHARED_META_RESERVED_SCN_TNAME));
  for (int64_t i = 0; i <= RESIDUE_ROW_COUNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, insert_sql.append_fmt(
        "%s(%lu, '%s', %ld, NULL)",
        0 == i ? "" : ", ",
        OB_SYS_TENANT_ID, ls_ckpt_type.to_str(),
        static_cast<int64_t>(SCN_BASE + i)));
  }
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(insert_sql.ptr()));

  int64_t residue_count = 0;
  int64_t min_residue_val = INT64_MAX;
  bool found_future = false;

  // 第一轮：达到扫描上限，按递增序只删除最小的 1024 条。
  ASSERT_EQ(OB_SUCCESS,
      ObSSLsCkptFinishTask::remove_reserved_meta_scns_at_or_before_(SYS_LS, upper_bound_scn));
  ASSERT_EQ(OB_SUCCESS, count_residue_rows(
      residue_count, min_residue_val, found_future));
  ASSERT_EQ(RESIDUE_ROW_COUNT - SCAN_LIMIT_COUNT, residue_count);
  ASSERT_EQ(static_cast<int64_t>(SCN_BASE + SCAN_LIMIT_COUNT), min_residue_val);
  ASSERT_TRUE(found_future);

  // 第二轮：剩余残留全部删除，未来记录仍保留。
  ASSERT_EQ(OB_SUCCESS,
      ObSSLsCkptFinishTask::remove_reserved_meta_scns_at_or_before_(SYS_LS, upper_bound_scn));
  ASSERT_EQ(OB_SUCCESS, count_residue_rows(
      residue_count, min_residue_val, found_future));
  ASSERT_EQ(0, residue_count);
  ASSERT_TRUE(found_future);
}

TEST_F(TestSharedMetaService, get_ls_ckpt_meta_scn_configured_mode)
{
  LOG_INFO("[TEST CASE BEGIN] get_ls_ckpt_meta_scn_configured_mode");
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  const ObLSID ls_id(8901);
  share::SCN meta_scn;
  ASSERT_EQ(OB_SUCCESS, make_scn(101, meta_scn));

  share::SCN result_scn = meta_scn;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            meta_svr->get_ls_ckpt_meta_scn(ObLSID(), result_scn));
  ASSERT_FALSE(result_scn.is_valid());

  {
    share::ObTenantSwitchGuard user_tenant_guard;
    ASSERT_EQ(OB_SUCCESS, user_tenant_guard.switch_to(run_ctx_.tenant_id_));
    ObSSMetaService *user_meta_svr = MTL(ObSSMetaService *);
    ASSERT_NE(nullptr, user_meta_svr);
    result_scn = meta_scn;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              user_meta_svr->get_ls_ckpt_meta_scn(ls_id, result_scn));
    ASSERT_FALSE(result_scn.is_valid());
  }

  if (!GCONF.enable_logservice) {
    result_scn = meta_scn;
    ASSERT_EQ(OB_NOT_SUPPORTED, meta_svr->get_ls_ckpt_meta_scn(ls_id, result_scn));
    ASSERT_FALSE(result_scn.is_valid());
  } else {
    char key_buf[SS_LS_CKPT_KEY_BUF_LEN] = {0};
    int64_t key_len = 0;
    ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::build_ls_ckpt_key(
        ls_id, key_buf, sizeof(key_buf), key_len));
    const ObString palf_key(static_cast<int32_t>(key_len), key_buf);
    sslog::ObSSLogKVPalfAdapter palf_kv_adapter;
    ASSERT_EQ(OB_SUCCESS, palf_kv_adapter.init(GCONF.cluster_id, OB_SYS_TENANT_ID));

    result_scn = meta_scn;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, meta_svr->get_ls_ckpt_meta_scn(ls_id, result_scn));
    ASSERT_FALSE(result_scn.is_valid());

    ObSSMetaLSCkptKey ckpt_key;
    ckpt_key.ls_id_ = ls_id;
    ckpt_key.meta_scn_ = meta_scn;
    const blocksstable::MacroBlockId ckpt_id =
        ckpt_key.convert_to_macro_id(false/*is_in_table*/);
    char value_buf[128] = {0};
    int64_t value_pos = 0;
    ASSERT_TRUE(ckpt_id.is_valid());
    ASSERT_EQ(OB_SUCCESS, ckpt_id.serialize(value_buf, sizeof(value_buf), value_pos));
    ASSERT_EQ(OB_SUCCESS, palf_kv_adapter.put(
        palf_key, ObString(static_cast<int32_t>(value_pos), value_buf)));

    ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_ckpt_meta_scn(ls_id, result_scn));
    ASSERT_EQ(meta_scn.get_val_for_inner_table_field(),
              result_scn.get_val_for_inner_table_field());

    ObSSMetaLSKey wrong_key;
    wrong_key.ls_id_ = ls_id;
    wrong_key.meta_scn_ = meta_scn;
    const blocksstable::MacroBlockId wrong_id =
        wrong_key.convert_to_macro_id(false/*is_in_table*/);
    MEMSET(value_buf, 0, sizeof(value_buf));
    value_pos = 0;
    ASSERT_TRUE(wrong_id.is_valid());
    ASSERT_EQ(OB_SUCCESS, wrong_id.serialize(value_buf, sizeof(value_buf), value_pos));
    ASSERT_EQ(OB_SUCCESS, palf_kv_adapter.set(
        palf_key, ObString(static_cast<int32_t>(value_pos), value_buf)));
    result_scn = meta_scn;
    ASSERT_EQ(OB_INVALID_DATA, meta_svr->get_ls_ckpt_meta_scn(ls_id, result_scn));
    ASSERT_FALSE(result_scn.is_valid());
    ASSERT_EQ(OB_SUCCESS, palf_kv_adapter.delete_kv(palf_key));
  }
  LOG_INFO("[TEST CASE END] get_ls_ckpt_meta_scn_configured_mode");
}

TEST_F(TestSharedMetaService, get_ls_ckpt_meta_scn_palf_key_not_exist)
{
  share::ObTenantSwitchGuard tenant_guard;
  ObSSMetaService *meta_svr = nullptr;
  ObString palf_key;
  const ObLSID ls_id(8902);
  char key_buf[SS_LS_CKPT_KEY_BUF_LEN] = {0};
  int64_t key_len = 0;
  share::SCN result_scn;
  SysLsCkptPauseGuard pause_guard;
  MockLsCkptPalfAdapter mock_adapter;
  MockLsCkptPalfAdapter *saved_mock = MockLsCkptPalfAdapter::get_active_mock();
  const bool saved_enable_logservice = GCONF.enable_logservice;
  int ret = OB_SUCCESS;

  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  ASSERT_EQ(OB_SUCCESS, pause_guard.pause());
  ASSERT_EQ(OB_SUCCESS, make_scn(101, result_scn));
  ASSERT_TRUE(result_scn.is_valid());
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::build_ls_ckpt_key(
      ls_id, key_buf, sizeof(key_buf), key_len));
  palf_key.assign_ptr(key_buf, static_cast<int32_t>(key_len));
  EXPECT_CALL(mock_adapter, get(palf_key, ::testing::_))
      .Times(1)
      .WillOnce(::testing::Return(OB_ENTRY_NOT_EXIST));

  // 不依赖默认配置；只在同步读取期间开启分支，返回后立即恢复配置和线程脚本。
  {
    DEFER(GCONF.enable_logservice = saved_enable_logservice);
    DEFER(MockLsCkptPalfAdapter::set_active_mock(saved_mock));
    MockLsCkptPalfAdapter::set_active_mock(&mock_adapter);
    GCONF.enable_logservice = true;
    ret = meta_svr->get_ls_ckpt_meta_scn(ls_id, result_scn);
  }
  ASSERT_EQ(saved_enable_logservice, static_cast<bool>(GCONF.enable_logservice));
  ASSERT_EQ(saved_mock, MockLsCkptPalfAdapter::get_active_mock());
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ASSERT_FALSE(result_scn.is_valid());
}

TEST_F(TestSharedMetaService, get_ls_ckpt_meta_scn_palf_invalid_macro_id)
{
  share::ObTenantSwitchGuard tenant_guard;
  ObSSMetaService *meta_svr = nullptr;
  ObString palf_key;
  const ObLSID ls_id(8903);
  char key_buf[SS_LS_CKPT_KEY_BUF_LEN] = {0};
  char value_buf[128] = {0};
  int64_t key_len = 0;
  int64_t value_pos = 0;
  int64_t read_pos = 0;
  ObSSMetaLSKey wrong_key;
  blocksstable::MacroBlockId wrong_id;
  blocksstable::MacroBlockId decoded_id;
  share::SCN result_scn;
  SysLsCkptPauseGuard pause_guard;
  MockLsCkptPalfAdapter mock_adapter;
  MockLsCkptPalfAdapter *saved_mock = MockLsCkptPalfAdapter::get_active_mock();
  const bool saved_enable_logservice = GCONF.enable_logservice;
  int ret = OB_SUCCESS;

  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  ASSERT_EQ(OB_SUCCESS, pause_guard.pause());
  ASSERT_EQ(OB_SUCCESS, make_scn(102, result_scn));
  ASSERT_TRUE(result_scn.is_valid());
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::build_ls_ckpt_key(
      ls_id, key_buf, sizeof(key_buf), key_len));
  palf_key.assign_ptr(key_buf, static_cast<int32_t>(key_len));

  // 提供可成功反序列化的 LS_META ID，确保失败来自真实 MetaService 的类型校验。
  wrong_key.ls_id_ = ls_id;
  wrong_key.meta_scn_ = result_scn;
  wrong_id = wrong_key.convert_to_macro_id(false/*is_in_table*/);
  ASSERT_TRUE(wrong_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, wrong_id.serialize(value_buf, sizeof(value_buf), value_pos));
  ASSERT_EQ(OB_SUCCESS, decoded_id.deserialize(value_buf, value_pos, read_pos));
  ASSERT_EQ(value_pos, read_pos);
  ASSERT_TRUE(decoded_id.is_valid());
  ASSERT_NE(ObStorageObjectType::SHARED_LS_CKPT, decoded_id.storage_object_type());
  mock_adapter.set_value(ObString(static_cast<int32_t>(value_pos), value_buf));
  EXPECT_CALL(mock_adapter, get(palf_key, ::testing::_))
      .Times(1)
      .WillOnce(::testing::Invoke(&mock_adapter, &MockLsCkptPalfAdapter::fill_value));

  {
    DEFER(GCONF.enable_logservice = saved_enable_logservice);
    DEFER(MockLsCkptPalfAdapter::set_active_mock(saved_mock));
    MockLsCkptPalfAdapter::set_active_mock(&mock_adapter);
    GCONF.enable_logservice = true;
    ret = meta_svr->get_ls_ckpt_meta_scn(ls_id, result_scn);
  }
  ASSERT_EQ(saved_enable_logservice, static_cast<bool>(GCONF.enable_logservice));
  ASSERT_EQ(saved_mock, MockLsCkptPalfAdapter::get_active_mock());
  ASSERT_EQ(OB_INVALID_DATA, ret);
  ASSERT_FALSE(result_scn.is_valid());
}

TEST_F(TestSharedMetaService, read_ls_ckpt_data_basic_and_reset_on_error)
{
  LOG_INFO("[TEST CASE BEGIN] read_ls_ckpt_data_basic_and_reset_on_error");
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  const ObLSID ls_id = share::SYS_LS;
  ObSSLSMeta source_ls_meta;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(ls_id, source_ls_meta));
  ASSERT_TRUE(source_ls_meta.is_valid());

  share::SCN ckpt_scn;
  ASSERT_EQ(OB_SUCCESS, make_scn(910100, ckpt_scn));
  share::SCN reserved_meta_scn;
  ASSERT_EQ(OB_SUCCESS, make_scn(910050, reserved_meta_scn));
  ObSSLsCkptData ckpt_data;
  ckpt_data.ls_meta_ = source_ls_meta;
  ckpt_data.ckpt_scn_ = ckpt_scn;
  ckpt_data.ckpt_ts_ = ObTimeUtility::current_time();
  ckpt_data.reserved_meta_scn_ = reserved_meta_scn;

  ObSSMetaTabletKey tablet_key1;
  tablet_key1.ls_id_ = ls_id;
  tablet_key1.tablet_id_ = ObTabletID(9910001);
  tablet_key1.reorganization_scn_ = share::SCN::min_scn();
  ASSERT_EQ(OB_SUCCESS, make_scn(910011, tablet_key1.meta_scn_));
  const blocksstable::MacroBlockId tablet_id1 =
      tablet_key1.convert_to_macro_id(false/*is_in_table*/);
  ASSERT_TRUE(tablet_id1.is_valid());
  ASSERT_EQ(OB_SUCCESS, ckpt_data.add_tablet_meta_key(tablet_id1));

  ObSSMetaTabletKey tablet_key2;
  tablet_key2.ls_id_ = ls_id;
  tablet_key2.tablet_id_ = ObTabletID(9910002);
  tablet_key2.reorganization_scn_ = share::SCN::min_scn();
  ASSERT_EQ(OB_SUCCESS, make_scn(910012, tablet_key2.meta_scn_));
  const blocksstable::MacroBlockId tablet_id2 =
      tablet_key2.convert_to_macro_id(false/*is_in_table*/);
  ASSERT_TRUE(tablet_id2.is_valid());
  ASSERT_EQ(OB_SUCCESS, ckpt_data.add_tablet_meta_key(tablet_id2));
  ASSERT_TRUE(ckpt_data.is_valid());

  ObSSMetaLSCkptKey ckpt_key;
  ckpt_key.ls_id_ = ls_id;
  ckpt_key.meta_scn_ = ckpt_scn;
  const blocksstable::MacroBlockId ckpt_id =
      ckpt_key.convert_to_macro_id(false/*is_in_table*/);
  ASSERT_TRUE(ckpt_id.is_valid());

  ObArenaAllocator allocator("ReadCkptData");
  const int64_t serialize_size = ckpt_data.get_serialize_size();
  char *serialize_buf = static_cast<char *>(allocator.alloc(serialize_size));
  ASSERT_NE(nullptr, serialize_buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ckpt_data.serialize(serialize_buf, serialize_size, pos));
  ASSERT_EQ(serialize_size, pos);
  ASSERT_EQ(OB_SUCCESS, delete_test_object(ckpt_id));
  ASSERT_EQ(OB_SUCCESS, write_test_object(
      ckpt_id, serialize_buf, serialize_size, false/*need_align*/));

  ObSSLSMeta read_ls_meta;
  ObSEArray<blocksstable::MacroBlockId, 4> read_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, meta_svr->read_ls_ckpt_data(
      ls_id, ckpt_scn, read_ls_meta, read_tablet_ids));
  ASSERT_TRUE(read_ls_meta.is_valid());
  ASSERT_EQ(ls_id, read_ls_meta.ls_id_);
  ASSERT_EQ(2, read_tablet_ids.count());
  ASSERT_TRUE(tablet_id1 == read_tablet_ids.at(0));
  ASSERT_TRUE(tablet_id2 == read_tablet_ids.at(1));

  ASSERT_EQ(OB_SUCCESS, delete_test_object(ckpt_id));
  read_ls_meta = source_ls_meta;
  ASSERT_EQ(OB_SUCCESS, read_tablet_ids.push_back(tablet_id1));
  ASSERT_NE(OB_SUCCESS, meta_svr->read_ls_ckpt_data(
      ls_id, ckpt_scn, read_ls_meta, read_tablet_ids));
  ASSERT_FALSE(read_ls_meta.is_valid());
  ASSERT_TRUE(read_tablet_ids.empty());
  LOG_INFO("[TEST CASE END] read_ls_ckpt_data_basic_and_reset_on_error");
}

TEST_F(TestSharedMetaService, get_ls_ckpt_iter_snapshot_order_and_mark_delete)
{
  LOG_INFO("[TEST CASE BEGIN] get_ls_ckpt_iter_snapshot_order_and_mark_delete");
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  const ObLSID ls_id(8902);
  const ObLSID other_ls_id(8905);
  share::SCN scn100;
  share::SCN scn150;
  share::SCN scn200;
  share::SCN scn300;
  share::SCN scn50;
  ASSERT_EQ(OB_SUCCESS, make_scn(100, scn100));
  ASSERT_EQ(OB_SUCCESS, make_scn(150, scn150));
  ASSERT_EQ(OB_SUCCESS, make_scn(200, scn200));
  ASSERT_EQ(OB_SUCCESS, make_scn(300, scn300));
  ASSERT_EQ(OB_SUCCESS, make_scn(50, scn50));
  ASSERT_EQ(OB_SUCCESS, insert_ls_ckpt_row(*meta_svr, ls_id, scn100, false/*mark_delete*/));
  ASSERT_EQ(OB_SUCCESS, insert_ls_ckpt_row(*meta_svr, ls_id, scn150, true/*mark_delete*/));
  ASSERT_EQ(OB_SUCCESS, insert_ls_ckpt_row(*meta_svr, ls_id, scn200, false/*mark_delete*/));
  ASSERT_EQ(OB_SUCCESS, insert_ls_ckpt_row(*meta_svr, ls_id, scn300, false/*mark_delete*/));
  ASSERT_EQ(OB_SUCCESS, insert_ls_ckpt_row(
      *meta_svr, other_ls_id, scn150, false/*mark_delete*/));

  ObSSMetaIterGuard<ObSSMetaLSCkptKeyIter> iter_guard;
  ObSSMetaLSCkptKeyIter *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_ckpt_iter(
      ls_id, scn200, false/*read_mark_delete*/, iter_guard));
  ASSERT_EQ(OB_SUCCESS, iter_guard.get_iter(iter));
  ASSERT_NE(nullptr, iter);
  ObSSMetaLSCkptKey key;
  ASSERT_EQ(OB_SUCCESS, iter->get_next(key));
  ASSERT_EQ(ls_id, key.ls_id_);
  ASSERT_EQ(scn200, key.meta_scn_);
  ASSERT_EQ(OB_SUCCESS, iter->get_next(key));
  ASSERT_EQ(ls_id, key.ls_id_);
  ASSERT_EQ(scn100, key.meta_scn_);
  ASSERT_EQ(OB_ITER_END, iter->get_next(key));

  ObSSMetaIterGuard<ObSSMetaLSCkptKeyIter> mark_delete_iter_guard;
  ObSSMetaLSCkptKeyIter *mark_delete_iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_ckpt_iter(
      ls_id, scn200, true/*read_mark_delete*/, mark_delete_iter_guard));
  ASSERT_EQ(OB_SUCCESS, mark_delete_iter_guard.get_iter(mark_delete_iter));
  ASSERT_NE(nullptr, mark_delete_iter);
  ASSERT_EQ(OB_SUCCESS, mark_delete_iter->get_next(key));
  ASSERT_EQ(scn200, key.meta_scn_);
  ASSERT_EQ(OB_SUCCESS, mark_delete_iter->get_next(key));
  ASSERT_EQ(scn150, key.meta_scn_);
  ASSERT_EQ(OB_SUCCESS, mark_delete_iter->get_next(key));
  ASSERT_EQ(scn100, key.meta_scn_);
  ASSERT_EQ(OB_ITER_END, mark_delete_iter->get_next(key));

  ObSSMetaIterGuard<ObSSMetaLSCkptKeyIter> empty_iter_guard;
  ObSSMetaLSCkptKeyIter *empty_iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_ckpt_iter(
      ls_id, scn50, false/*read_mark_delete*/, empty_iter_guard));
  ASSERT_EQ(OB_SUCCESS, empty_iter_guard.get_iter(empty_iter));
  ASSERT_NE(nullptr, empty_iter);
  ASSERT_EQ(OB_ITER_END, empty_iter->get_next(key));

  ObSSMetaIterGuard<ObSSMetaLSCkptKeyIter> invalid_iter_guard;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            meta_svr->get_ls_ckpt_iter(
                ObLSID(), scn200, false/*read_mark_delete*/, invalid_iter_guard));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            meta_svr->get_ls_ckpt_iter(
                ls_id, share::SCN::invalid_scn(), false/*read_mark_delete*/, invalid_iter_guard));

  ASSERT_EQ(OB_SUCCESS, delete_ls_ckpt_row(*meta_svr, ls_id, scn100));
  ASSERT_EQ(OB_SUCCESS, delete_ls_ckpt_row(*meta_svr, ls_id, scn150));
  ASSERT_EQ(OB_SUCCESS, delete_ls_ckpt_row(*meta_svr, ls_id, scn200));
  ASSERT_EQ(OB_SUCCESS, delete_ls_ckpt_row(*meta_svr, ls_id, scn300));
  ASSERT_EQ(OB_SUCCESS, delete_ls_ckpt_row(*meta_svr, other_ls_id, scn150));
  LOG_INFO("[TEST CASE END] get_ls_ckpt_iter_snapshot_order_and_mark_delete");
}

TEST_F(TestSharedMetaService, batch_del_ls_ckpt_double_delete)
{
  LOG_INFO("[TEST CASE BEGIN] batch_del_ls_ckpt_double_delete");
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  const ObLSID ls_id(8903);
  share::SCN scn100;
  share::SCN scn200;
  ASSERT_EQ(OB_SUCCESS, make_scn(100, scn100));
  ASSERT_EQ(OB_SUCCESS, make_scn(200, scn200));

  ObArray<share::SCN> empty_scns;
  ASSERT_EQ(OB_INVALID_ARGUMENT, meta_svr->batch_del_ls_ckpt(ls_id, empty_scns));

  ObArray<share::SCN> scns;
  ASSERT_EQ(OB_SUCCESS, scns.push_back(scn100));
  ASSERT_EQ(OB_SUCCESS, scns.push_back(scn200));
  ASSERT_EQ(OB_SUCCESS, insert_ls_ckpt_row(*meta_svr, ls_id, scn100, false/*mark_delete*/));
  ASSERT_EQ(OB_SUCCESS, insert_ls_ckpt_row(*meta_svr, ls_id, scn200, false/*mark_delete*/));

  ObSSMetaLSCkptKey key100;
  key100.ls_id_ = ls_id;
  key100.meta_scn_ = scn100;
  const blocksstable::MacroBlockId object100 =
      key100.convert_to_macro_id(false/*is_in_table*/);
  ObSSMetaLSCkptKey key200;
  key200.ls_id_ = ls_id;
  key200.meta_scn_ = scn200;
  const blocksstable::MacroBlockId object200 =
      key200.convert_to_macro_id(false/*is_in_table*/);
  const char object_data[] = "ls_ckpt_delete_test";
  ASSERT_EQ(OB_SUCCESS, delete_test_object(object100));
  ASSERT_EQ(OB_SUCCESS, delete_test_object(object200));
  ASSERT_EQ(OB_SUCCESS, write_test_object(
      object100, object_data, sizeof(object_data), false/*need_align*/));
  ASSERT_EQ(OB_SUCCESS, write_test_object(
      object200, object_data, sizeof(object_data), false/*need_align*/));

  // simple_server 使用当前数据版本，双删应由版本门禁开启，不依赖仅在
  // ERRSIM 构建中注册的 EN_SS_FORCE_DOUBLE_WRITE_META。
  bool need_double_delete = false;
  ASSERT_EQ(OB_SUCCESS, check_double_route_ss_meta(MTL_ID(), need_double_delete));
  ASSERT_TRUE(need_double_delete);
  bool exist = true;

  // The whole batch must be validated before deletion; an invalid trailing
  // item cannot leave the valid prefix partially deleted.
  ObArray<share::SCN> invalid_scns;
  ASSERT_EQ(OB_SUCCESS, invalid_scns.push_back(scn100));
  ASSERT_EQ(OB_SUCCESS, invalid_scns.push_back(share::SCN::invalid_scn()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, meta_svr->batch_del_ls_ckpt(ls_id, invalid_scns));
  ASSERT_EQ(OB_SUCCESS, check_ls_ckpt_row_exist(*meta_svr, ls_id, scn100, exist));
  ASSERT_TRUE(exist);
  ASSERT_EQ(OB_SUCCESS, check_test_object_exist(object100, exist));
  ASSERT_TRUE(exist);

  share::SCN published_scn;
  share::SCN future_scn;
  ASSERT_EQ(OB_SUCCESS, make_scn(1000, published_scn));
  ASSERT_EQ(OB_SUCCESS, make_scn(1100, future_scn));
  ObSSMetaLSCkptKey published_key;
  published_key.ls_id_ = ls_id;
  published_key.meta_scn_ = published_scn;
  const blocksstable::MacroBlockId published_object =
      published_key.convert_to_macro_id(false/*is_in_table*/);
  ObSSMetaLSCkptKey future_key;
  future_key.ls_id_ = ls_id;
  future_key.meta_scn_ = future_scn;
  const blocksstable::MacroBlockId future_object =
      future_key.convert_to_macro_id(false/*is_in_table*/);
  sslog::ObSSLogKVPalfAdapter palf_kv_adapter;
  char palf_key_buf[SS_LS_CKPT_KEY_BUF_LEN] = {0};
  int64_t palf_key_len = 0;
  ObString palf_key;
  bool palf_adapter_inited = false;
  DEFER({
    if (palf_adapter_inited) {
      const int tmp_ret = palf_kv_adapter.delete_kv(palf_key);
      EXPECT_TRUE(OB_SUCCESS == tmp_ret || OB_ENTRY_NOT_EXIST == tmp_ret);
    }
  });

  if (GCONF.enable_logservice) {
    ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::build_ls_ckpt_key(
        ls_id, palf_key_buf, sizeof(palf_key_buf), palf_key_len));
    palf_key.assign_ptr(palf_key_buf, static_cast<int32_t>(palf_key_len));
    ASSERT_EQ(OB_SUCCESS, palf_kv_adapter.init(GCONF.cluster_id, OB_SYS_TENANT_ID));
    palf_adapter_inited = true;
    const int cleanup_ret = palf_kv_adapter.delete_kv(palf_key);
    ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);
    ObArenaAllocator missing_kv_alloc("MissCkptKv");
    ObStringBuffer missing_kv_value(&missing_kv_alloc);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, palf_kv_adapter.get(palf_key, missing_kv_value));

    // Before the first PALF_KV publication, deletion fails closed because a
    // checkpoint may already be uploaded and about to be published.
    ASSERT_EQ(OB_EAGAIN, meta_svr->batch_del_ls_ckpt(ls_id, scns));
    ASSERT_EQ(OB_SUCCESS, check_ls_ckpt_row_exist(*meta_svr, ls_id, scn100, exist));
    ASSERT_TRUE(exist);
    ASSERT_EQ(OB_SUCCESS, check_test_object_exist(object100, exist));
    ASSERT_TRUE(exist);

    ASSERT_EQ(OB_SUCCESS, insert_ls_ckpt_row(
        *meta_svr, ls_id, published_scn, false/*mark_delete*/));
    ASSERT_EQ(OB_SUCCESS, insert_ls_ckpt_row(
        *meta_svr, ls_id, future_scn, false/*mark_delete*/));
    ASSERT_EQ(OB_SUCCESS, delete_test_object(published_object));
    ASSERT_EQ(OB_SUCCESS, delete_test_object(future_object));
    ASSERT_EQ(OB_SUCCESS, write_test_object(
        published_object, object_data, sizeof(object_data), false/*need_align*/));
    ASSERT_EQ(OB_SUCCESS, write_test_object(
        future_object, object_data, sizeof(object_data), false/*need_align*/));

    char palf_value_buf[128] = {0};
    int64_t palf_value_len = 0;
    ASSERT_EQ(OB_SUCCESS, published_object.serialize(
        palf_value_buf, sizeof(palf_value_buf), palf_value_len));
    ASSERT_EQ(OB_SUCCESS, palf_kv_adapter.put(
        palf_key, ObString(static_cast<int32_t>(palf_value_len), palf_value_buf)));

    // The current version in the second position proves the protection is a
    // full-batch preflight: the older prefix must remain untouched.
    ObArray<share::SCN> contains_published_scns;
    ASSERT_EQ(OB_SUCCESS, contains_published_scns.push_back(scn100));
    ASSERT_EQ(OB_SUCCESS, contains_published_scns.push_back(published_scn));
    ASSERT_EQ(OB_EAGAIN,
              meta_svr->batch_del_ls_ckpt(ls_id, contains_published_scns));
    ASSERT_EQ(OB_SUCCESS, check_ls_ckpt_row_exist(*meta_svr, ls_id, scn100, exist));
    ASSERT_TRUE(exist);
    ASSERT_EQ(OB_SUCCESS, check_test_object_exist(object100, exist));
    ASSERT_TRUE(exist);
    ASSERT_EQ(OB_SUCCESS,
              check_ls_ckpt_row_exist(*meta_svr, ls_id, published_scn, exist));
    ASSERT_TRUE(exist);
    ASSERT_EQ(OB_SUCCESS, check_test_object_exist(published_object, exist));
    ASSERT_TRUE(exist);

    ObArray<share::SCN> future_scns;
    ASSERT_EQ(OB_SUCCESS, future_scns.push_back(future_scn));
    ASSERT_EQ(OB_EAGAIN, meta_svr->batch_del_ls_ckpt(ls_id, future_scns));
    ASSERT_EQ(OB_SUCCESS, check_ls_ckpt_row_exist(*meta_svr, ls_id, future_scn, exist));
    ASSERT_TRUE(exist);
    ASSERT_EQ(OB_SUCCESS, check_test_object_exist(future_object, exist));
    ASSERT_TRUE(exist);

    // Once PALF_KV advances, the formerly published version becomes eligible
    // for deletion while the new current version remains protected.
    char future_value_buf[128] = {0};
    int64_t future_value_len = 0;
    ASSERT_EQ(OB_SUCCESS, future_object.serialize(
        future_value_buf, sizeof(future_value_buf), future_value_len));
    ASSERT_EQ(OB_SUCCESS, palf_kv_adapter.set(
        palf_key, ObString(static_cast<int32_t>(future_value_len), future_value_buf)));
    ObArray<share::SCN> old_published_scns;
    ASSERT_EQ(OB_SUCCESS, old_published_scns.push_back(published_scn));
    ASSERT_EQ(OB_SUCCESS, meta_svr->batch_del_ls_ckpt(ls_id, old_published_scns));
    ASSERT_EQ(OB_SUCCESS,
              check_ls_ckpt_row_exist(*meta_svr, ls_id, published_scn, exist));
    ASSERT_FALSE(exist);
    ASSERT_EQ(OB_SUCCESS, check_test_object_exist(published_object, exist));
    ASSERT_FALSE(exist);
    ASSERT_EQ(OB_EAGAIN, meta_svr->batch_del_ls_ckpt(ls_id, future_scns));
    ASSERT_EQ(OB_SUCCESS, check_ls_ckpt_row_exist(*meta_svr, ls_id, future_scn, exist));
    ASSERT_TRUE(exist);
    ASSERT_EQ(OB_SUCCESS, check_test_object_exist(future_object, exist));
    ASSERT_TRUE(exist);
  }

  ASSERT_EQ(OB_SUCCESS, meta_svr->batch_del_ls_ckpt(ls_id, scns));
  ASSERT_EQ(OB_SUCCESS, check_ls_ckpt_row_exist(*meta_svr, ls_id, scn100, exist));
  ASSERT_FALSE(exist);
  ASSERT_EQ(OB_SUCCESS, check_ls_ckpt_row_exist(*meta_svr, ls_id, scn200, exist));
  ASSERT_FALSE(exist);
  ASSERT_EQ(OB_SUCCESS, check_test_object_exist(object100, exist));
  ASSERT_FALSE(exist);
  ASSERT_EQ(OB_SUCCESS, check_test_object_exist(object200, exist));
  ASSERT_FALSE(exist);

  // A retry must tolerate missing objects and still finish the table cleanup.
  ASSERT_EQ(OB_SUCCESS, meta_svr->batch_del_ls_ckpt(ls_id, scns));

  // 表删除失败时，对象已经先被删除；随后重试会把对象不存在归一化为成功，
  // 并继续删除遗留的内部表行。
  share::SCN scn300;
  ASSERT_EQ(OB_SUCCESS, make_scn(300, scn300));
  ASSERT_EQ(OB_SUCCESS, insert_ls_ckpt_row(*meta_svr, ls_id, scn300, false/*mark_delete*/));
  ObSSMetaLSCkptKey key300;
  key300.ls_id_ = ls_id;
  key300.meta_scn_ = scn300;
  const blocksstable::MacroBlockId object300 =
      key300.convert_to_macro_id(false/*is_in_table*/);
  ASSERT_EQ(OB_SUCCESS, delete_test_object(object300));
  ASSERT_EQ(OB_SUCCESS, write_test_object(
      object300, object_data, sizeof(object_data), false/*need_align*/));
  ObArray<share::SCN> scns300;
  ASSERT_EQ(OB_SUCCESS, scns300.push_back(scn300));
  {
    NamedEventGuard table_delete_error;
    ASSERT_EQ(OB_SUCCESS, table_delete_error.init(
        "EN_SS_LS_CKPT_TABLE_DELETE_ERROR", OB_IO_ERROR, 1));
    ASSERT_EQ(OB_IO_ERROR, meta_svr->batch_del_ls_ckpt(ls_id, scns300));
  }
  ASSERT_EQ(OB_SUCCESS, check_ls_ckpt_row_exist(*meta_svr, ls_id, scn300, exist));
  ASSERT_TRUE(exist);
  ASSERT_EQ(OB_SUCCESS, check_test_object_exist(object300, exist));
  ASSERT_FALSE(exist);
  {
    NamedEventGuard remote_not_exist;
    ASSERT_EQ(OB_SUCCESS, remote_not_exist.init(
        "EN_SS_LS_CKPT_REMOTE_DELETE_ERROR", OB_OBJECT_NOT_EXIST, 1));
    ASSERT_EQ(OB_SUCCESS, meta_svr->batch_del_ls_ckpt(ls_id, scns300));
  }
  ASSERT_EQ(OB_SUCCESS, check_ls_ckpt_row_exist(*meta_svr, ls_id, scn300, exist));
  ASSERT_FALSE(exist);

  // 普通对象存储错误必须在删除内部表前返回，且不得误删对象或表行。
  share::SCN scn400;
  ASSERT_EQ(OB_SUCCESS, make_scn(400, scn400));
  ASSERT_EQ(OB_SUCCESS, insert_ls_ckpt_row(*meta_svr, ls_id, scn400, false/*mark_delete*/));
  ObSSMetaLSCkptKey key400;
  key400.ls_id_ = ls_id;
  key400.meta_scn_ = scn400;
  const blocksstable::MacroBlockId object400 =
      key400.convert_to_macro_id(false/*is_in_table*/);
  ASSERT_EQ(OB_SUCCESS, delete_test_object(object400));
  ASSERT_EQ(OB_SUCCESS, write_test_object(
      object400, object_data, sizeof(object_data), false/*need_align*/));
  ObArray<share::SCN> scns400;
  ASSERT_EQ(OB_SUCCESS, scns400.push_back(scn400));
  {
    NamedEventGuard remote_delete_error;
    ASSERT_EQ(OB_SUCCESS, remote_delete_error.init(
        "EN_SS_LS_CKPT_REMOTE_DELETE_ERROR", OB_IO_ERROR, 1));
    ASSERT_EQ(OB_IO_ERROR, meta_svr->batch_del_ls_ckpt(ls_id, scns400));
  }
  ASSERT_EQ(OB_SUCCESS, check_ls_ckpt_row_exist(*meta_svr, ls_id, scn400, exist));
  ASSERT_TRUE(exist);
  ASSERT_EQ(OB_SUCCESS, check_test_object_exist(object400, exist));
  ASSERT_TRUE(exist);
  ASSERT_EQ(OB_SUCCESS, meta_svr->batch_del_ls_ckpt(ls_id, scns400));
  ASSERT_EQ(OB_SUCCESS, check_ls_ckpt_row_exist(*meta_svr, ls_id, scn400, exist));
  ASSERT_FALSE(exist);
  ASSERT_EQ(OB_SUCCESS, check_test_object_exist(object400, exist));
  ASSERT_FALSE(exist);

  if (GCONF.enable_logservice) {
    ASSERT_TRUE(palf_adapter_inited);
    ASSERT_EQ(OB_SUCCESS, palf_kv_adapter.delete_kv(palf_key));
    ASSERT_EQ(OB_SUCCESS, delete_ls_ckpt_row(*meta_svr, ls_id, future_scn));
    ASSERT_EQ(OB_SUCCESS, delete_test_object(future_object));
  }
  LOG_INFO("[TEST CASE END] batch_del_ls_ckpt_double_delete");
}

TEST_F(TestSharedMetaService, remove_tablet_double_delete_order_and_errors)
{
  LOG_INFO("[TEST CASE BEGIN] remove_tablet_double_delete_order_and_errors");
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  const ObLSID ls_id(8904);
  share::SCN reorgan_scn;
  ASSERT_EQ(OB_SUCCESS, make_scn(77, reorgan_scn));
  const char object_data[] = "tablet_meta_delete_test";

  auto prepare_both_sides = [&](const uint64_t tablet_id_val,
                                const uint64_t meta_scn_val,
                                ObSSMetaTabletKey &key,
                                blocksstable::MacroBlockId &object_id) {
    key.ls_id_ = ls_id;
    key.tablet_id_ = ObTabletID(tablet_id_val);
    key.reorganization_scn_ = reorgan_scn;
    ASSERT_EQ(OB_SUCCESS, make_scn(meta_scn_val, key.meta_scn_));
    ASSERT_TRUE(key.is_valid());
    ASSERT_EQ(OB_SUCCESS, insert_test_tablet_meta_row(*meta_svr, key));
    object_id = key.convert_to_macro_id(false/*is_in_table*/);
    ASSERT_TRUE(object_id.is_valid());
    ASSERT_EQ(OB_SUCCESS, delete_test_object(object_id));
    ASSERT_EQ(OB_SUCCESS, write_test_object(
        object_id, object_data, sizeof(object_data), false/*need_align*/));
  };
  auto exact_range = [](const share::SCN &meta_scn) {
    share::ObScnRange range;
    range.start_scn_ = meta_scn;
    range.end_scn_ = meta_scn;
    return range;
  };

  // simple_server 使用当前数据版本，双删应由版本门禁开启，不依赖仅在
  // ERRSIM 构建中注册的 EN_SS_FORCE_DOUBLE_WRITE_META。
  bool need_double_delete = false;
  ASSERT_EQ(OB_SUCCESS, check_double_route_ss_meta(MTL_ID(), need_double_delete));
  ASSERT_TRUE(need_double_delete);
  bool exist = true;

  // 同一 tablet 的闭区间会删除区间内全部版本，并保留两侧相邻版本。
  const uint64_t range_scns[] = {100, 200, 300, 400};
  ObSSMetaTabletKey range_keys[4];
  blocksstable::MacroBlockId range_objects[4];
  for (int64_t i = 0; i < 4; ++i) {
    prepare_both_sides(
        9910200, range_scns[i], range_keys[i], range_objects[i]);
  }
  share::ObScnRange remove_range;
  remove_range.start_scn_ = range_keys[1].meta_scn_;
  remove_range.end_scn_ = range_keys[2].meta_scn_;
  ASSERT_EQ(OB_SUCCESS, meta_svr->remove_tablet(
      ls_id, range_keys[0].tablet_id_, reorgan_scn, remove_range));
  for (int64_t i = 0; i < 4; ++i) {
    const bool expected_exist = (0 == i || 3 == i);
    ASSERT_EQ(OB_SUCCESS,
              check_tablet_meta_row_exist(*meta_svr, range_keys[i], exist));
    ASSERT_EQ(expected_exist, exist);
    ASSERT_EQ(OB_SUCCESS, check_test_object_exist(range_objects[i], exist));
    ASSERT_EQ(expected_exist, exist);
  }
  ASSERT_EQ(OB_SUCCESS, meta_svr->remove_tablet(
      ls_id, range_keys[0].tablet_id_, reorgan_scn,
      exact_range(range_keys[0].meta_scn_)));
  ASSERT_EQ(OB_SUCCESS, meta_svr->remove_tablet(
      ls_id, range_keys[3].tablet_id_, reorgan_scn,
      exact_range(range_keys[3].meta_scn_)));

  // 表删除失败时对象必须已经被删除；重试时对象不存在仍要继续删表。
  ObSSMetaTabletKey table_error_key;
  blocksstable::MacroBlockId table_error_object;
  prepare_both_sides(9910201, 500, table_error_key, table_error_object);
  {
    NamedEventGuard table_delete_error;
    ASSERT_EQ(OB_SUCCESS, table_delete_error.init(
        "EN_SS_TABLET_META_TABLE_DELETE_ERROR", OB_IO_ERROR, 1));
    ASSERT_EQ(OB_IO_ERROR, meta_svr->remove_tablet(
        ls_id, table_error_key.tablet_id_, reorgan_scn,
        exact_range(table_error_key.meta_scn_)));
  }
  ASSERT_EQ(OB_SUCCESS,
            check_test_object_exist(table_error_object, exist));
  ASSERT_FALSE(exist);
  ASSERT_EQ(OB_SUCCESS,
            check_tablet_meta_row_exist(*meta_svr, table_error_key, exist));
  ASSERT_TRUE(exist);
  {
    NamedEventGuard remote_not_exist;
    ASSERT_EQ(OB_SUCCESS, remote_not_exist.init(
        "EN_SS_TABLET_META_REMOTE_DELETE_ERROR", OB_OBJECT_NOT_EXIST, 1));
    ASSERT_EQ(OB_SUCCESS, meta_svr->remove_tablet(
        ls_id, table_error_key.tablet_id_, reorgan_scn,
        exact_range(table_error_key.meta_scn_)));
  }
  ASSERT_EQ(OB_SUCCESS,
            check_tablet_meta_row_exist(*meta_svr, table_error_key, exist));
  ASSERT_FALSE(exist);

  // 对象确实不存在时，OB_OBJECT_NOT_EXIST 必须转成功并继续删除内部表。
  ObSSMetaTabletKey missing_object_key;
  missing_object_key.ls_id_ = ls_id;
  missing_object_key.tablet_id_ = ObTabletID(9910202);
  missing_object_key.reorganization_scn_ = reorgan_scn;
  ASSERT_EQ(OB_SUCCESS, make_scn(600, missing_object_key.meta_scn_));
  ASSERT_EQ(OB_SUCCESS,
            insert_test_tablet_meta_row(*meta_svr, missing_object_key));
  const blocksstable::MacroBlockId missing_object =
      missing_object_key.convert_to_macro_id(false/*is_in_table*/);
  ASSERT_EQ(OB_SUCCESS, delete_test_object(missing_object));
  {
    NamedEventGuard remote_not_exist;
    ASSERT_EQ(OB_SUCCESS, remote_not_exist.init(
        "EN_SS_TABLET_META_REMOTE_DELETE_ERROR", OB_OBJECT_NOT_EXIST, 1));
    ASSERT_EQ(OB_SUCCESS, meta_svr->remove_tablet(
        ls_id, missing_object_key.tablet_id_, reorgan_scn,
        exact_range(missing_object_key.meta_scn_)));
  }
  ASSERT_EQ(OB_SUCCESS,
            check_tablet_meta_row_exist(*meta_svr, missing_object_key, exist));
  ASSERT_FALSE(exist);
  ASSERT_EQ(OB_SUCCESS, check_test_object_exist(missing_object, exist));
  ASSERT_FALSE(exist);

  // 普通对象存储错误必须阻断表删除，而且不能删除原对象。
  ObSSMetaTabletKey remote_error_key;
  blocksstable::MacroBlockId remote_error_object;
  prepare_both_sides(9910203, 700, remote_error_key, remote_error_object);
  {
    NamedEventGuard remote_delete_error;
    ASSERT_EQ(OB_SUCCESS, remote_delete_error.init(
        "EN_SS_TABLET_META_REMOTE_DELETE_ERROR", OB_IO_ERROR, 1));
    ASSERT_EQ(OB_IO_ERROR, meta_svr->remove_tablet(
        ls_id, remote_error_key.tablet_id_, reorgan_scn,
        exact_range(remote_error_key.meta_scn_)));
  }
  ASSERT_EQ(OB_SUCCESS,
            check_tablet_meta_row_exist(*meta_svr, remote_error_key, exist));
  ASSERT_TRUE(exist);
  ASSERT_EQ(OB_SUCCESS,
            check_test_object_exist(remote_error_object, exist));
  ASSERT_TRUE(exist);
  ASSERT_EQ(OB_SUCCESS, meta_svr->remove_tablet(
      ls_id, remote_error_key.tablet_id_, reorgan_scn,
      exact_range(remote_error_key.meta_scn_)));
  ASSERT_EQ(OB_SUCCESS,
            check_tablet_meta_row_exist(*meta_svr, remote_error_key, exist));
  ASSERT_FALSE(exist);
  ASSERT_EQ(OB_SUCCESS,
            check_test_object_exist(remote_error_object, exist));
  ASSERT_FALSE(exist);
  LOG_INFO("[TEST CASE END] remove_tablet_double_delete_order_and_errors");
}

/**
 * 概要：验证 NORMAL checkpoint 必须持有有效的 SSWriter 租约，BOOTSTRAP 不检查该租约。
 *
 * 覆盖模块：
 * 1. ObSSLsCkptModePolicy：区分两种模式是否需要检查 SSWriter。
 * 2. ObSSLsCkptWriteCtx::check_lease_valid：校验调度时记录的 SSWriter epoch。
 *
 * 测试流程：
 * 1. 使用当前 epoch 初始化 NORMAL Context，确认租约校验成功。
 * 2. 推进 epoch 后再次校验，确认返回 OB_NOT_MASTER。
 * 3. 初始化 BOOTSTRAP Context 并关闭 SSWriter，确认租约校验仍然成功。
 */
TEST_F(TestSharedMetaService, ls_ckpt_write_ctx_applies_mode_lease_policy)
{
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(OB_SYS_TENANT_ID));
  // Pause background checkpoints before advancing the lease epoch
  // to avoid disrupting in-flight DAGs.
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  const bool saved_sswriter_enabled = ATOMIC_LOAD(&mock_sswriter_enabled);
  const bool saved_is_sswriter = mock_is_sswriter;
  DEFER(ATOMIC_STORE(&mock_sswriter_enabled, saved_sswriter_enabled));
  DEFER(mock_is_sswriter = saved_is_sswriter);
  ATOMIC_STORE(&mock_sswriter_enabled, true);
  mock_is_sswriter = true;

  // NORMAL captures the initial epoch;
  // changing it must invalidate the existing context immediately.
  const int64_t captured_epoch = ATOMIC_LOAD(&lease_epoch);
  ObSSLsCkptWriteCtx normal_ctx;
  ASSERT_EQ(OB_SUCCESS, normal_ctx.init(ObSSLsCkptMode::NORMAL, captured_epoch, SYS_LS));
  ASSERT_EQ(OB_SUCCESS, normal_ctx.check_lease_valid());
  ATOMIC_INC(&lease_epoch);
  ASSERT_EQ(OB_NOT_MASTER, normal_ctx.check_lease_valid());

  // BOOTSTRAP does not require an SSWriter lease, so the check must pass even on a non-SSWriter.
  ObSSLsCkptWriteCtx bootstrap_ctx;
  ASSERT_EQ(OB_SUCCESS, bootstrap_ctx.init(ObSSLsCkptMode::BOOTSTRAP, -1, SYS_LS));
  ATOMIC_STORE(&mock_sswriter_enabled, false);
  mock_is_sswriter = false;
  ASSERT_EQ(OB_SUCCESS, bootstrap_ctx.check_lease_valid());
}

/**
 * Summary: verify that a BOOTSTRAP DAG writes a complete SYS LS checkpoint
 * without user-tenant data.
 *
 * Coverage:
 * 1. ObSSLsCkptExecutor: submit and wait for a checkpoint DAG through the BOOTSTRAP entry point.
 * 2. ObSSLsCkptDag: run the full Prepare, Upload, and Finish sequence.
 * 3. ObSSLsCkptReader and ObSSMetaService: locate and read back the checkpoint root object.
 * 4. Object storage: store SYS tablet meta and sub-meta while excluding user-tenant data.
 *
 * Steps:
 * 1. Prepare SYS-tenant and user-tenant tablets and record their object IDs.
 * 2. Run a complete BOOTSTRAP checkpoint and locate its root object.
 * 3. Read the root object from object storage and verify LS meta, SCN, and tablet object ID order.
 * 4. Verify that SYS tablet meta and sub-meta were uploaded and user-tenant objects were excluded.
 */
TEST_F(TestSharedMetaService, sys_ls_ckpt_write_dag_uploads_sys_meta_to_oss)
{
  // The checkpoint test path requires ERRSIM when logservice is disabled.
#ifndef ERRSIM
  if (!GCONF.enable_logservice) {
    SUCCEED() << "当前构建无 ERRSIM 且未启用 logservice，无法打开 checkpoint 测试通路";
    return;
  }
#endif
  LOG_INFO("[TEST CASE BEGIN] sys_ls_ckpt_write_dag_uploads_sys_meta_to_oss");
  const share::SCN reorgan_scn = share::SCN::min_scn();
  const int64_t table_suffix = ObTimeUtility::current_time();

  // Convert the in-table sub-meta ID from Tiered Store to an OSS ID
  // before checking the uploaded object.
  auto to_oss_sub_meta_id = [](const blocksstable::MacroBlockId &in_table_id) {
    blocksstable::MacroBlockId oss_id(in_table_id);
    oss_id.set_storage_object_type(
        static_cast<uint64_t>(blocksstable::ObStorageObjectType::SHARED_TABLET_SUB_META));
    oss_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
    return oss_id;
  };

  // Read the full object content rather than checking existence alone.
  auto read_oss_object = [](const blocksstable::MacroBlockId &macro_id,
                            const ObTabletID &effective_tablet_id,
                            ObIAllocator &allocator,
                            ObString &object_data) {
    int ret = OB_SUCCESS;
    int64_t object_size = 0;
    char *read_buf = nullptr;
    blocksstable::ObStorageObjectHandle read_handle;
    blocksstable::ObStorageObjectReadInfo read_info;

    object_data.reset();
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.get_object_size(macro_id, 0/*ls_epoch*/, object_size))) {
      LOG_WARN("get object size failed", KR(ret), K(macro_id));
    } else if (OB_UNLIKELY(object_size <= 0 || object_size > INT32_MAX)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid object size", KR(ret), K(macro_id), K(object_size));
    } else if (OB_ISNULL(read_buf = static_cast<char *>(allocator.alloc(object_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc read buffer failed", KR(ret), K(object_size), K(macro_id));
    } else {
      read_info.macro_block_id_ = macro_id;
      read_info.io_desc_.set_wait_event(1);
      read_info.buf_ = read_buf;
      read_info.offset_ = 0;
      read_info.size_ = object_size;
      read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
      read_info.mtl_tenant_id_ = MTL_ID();
      if (effective_tablet_id.is_valid()) {
        read_info.set_effective_tablet_id(effective_tablet_id);
      }
      if (OB_FAIL(OB_STORAGE_OBJECT_MGR.read_object(read_info, read_handle))) {
        LOG_WARN("read object failed", KR(ret), K(macro_id), K(read_info));
      } else if (OB_ISNULL(read_handle.get_buffer())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("empty object handle buffer", KR(ret), K(macro_id));
      } else if (OB_UNLIKELY(read_handle.get_data_size() <= 0
                             || read_handle.get_data_size() > object_size
                             || read_handle.get_data_size() > INT32_MAX)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid read object data size", KR(ret), K(macro_id),
                 K(object_size), "data_size", read_handle.get_data_size());
      } else {
        MEMCPY(read_buf, read_handle.get_buffer(), read_handle.get_data_size());
        object_data.assign_ptr(read_buf, static_cast<int32_t>(read_handle.get_data_size()));
      }
    }
    return ret;
  };

  // Prepare SYS-tenant objects that the checkpoint must collect and upload.
  ObTabletID sys_tablet_id;
  ObLSID sys_ls_id;
  share::SCN sys_tablet_meta_scn;
  blocksstable::MacroBlockId sys_tablet_meta_id;
  ObArray<blocksstable::MacroBlockId> sys_sub_meta_ids;
  {
    share::ObTenantSwitchGuard sys_guard;
    OK(sys_guard.switch_to(OB_SYS_TENANT_ID));

    ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
    ASSERT_NE(nullptr, meta_svr);
    INIT_SYS_TIERED_STORE_V2(sys_tiered_store);

    ASSERT_EQ(OB_SUCCESS, sys_exe_sql("create database if not exists ss_meta_test"));
    ObSqlString sys_table_name;
    ObSqlString create_sys_table_sql;
    ASSERT_EQ(OB_SUCCESS, sys_table_name.assign_fmt("sys_ls_ckpt_tbl_%ld", table_suffix));
    ASSERT_EQ(OB_SUCCESS, create_sys_table_sql.assign_fmt(
        "create table ss_meta_test.%s (a int)", sys_table_name.ptr()));
    ASSERT_EQ(OB_SUCCESS, sys_exe_sql(create_sys_table_sql.ptr()));
    ASSERT_EQ(OB_SUCCESS, get_sys_tablet_and_ls_id(sys_table_name.ptr(), sys_tablet_id, sys_ls_id));
    ASSERT_TRUE(sys_tablet_id.is_valid());
    ASSERT_TRUE(sys_ls_id.is_valid());
    LOG_INFO("got sys tablet_id and ls_id", K(sys_tablet_id), K(sys_ls_id));

    CLEAN_SYS_TABLET_META(meta_svr, sys_tiered_store, sys_ls_id, sys_tablet_id, reorgan_scn);
    ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(sys_ls_id, sys_tablet_id,
                                                  reorgan_scn, true/*check_lease*/));
    ObArenaAllocator tablet_alloc("LsCkptSysTab");
    ObTabletHandle tablet_hdl;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(sys_ls_id, sys_tablet_id,
                                                reorgan_scn, tablet_alloc,
                                                tablet_hdl, sys_tablet_meta_scn));
    ASSERT_TRUE(tablet_hdl.is_valid());
    ASSERT_TRUE(sys_tablet_meta_scn.is_valid());

    ObSSMetaTabletKey sys_tablet_key;
    sys_tablet_key.ls_id_ = sys_ls_id;
    sys_tablet_key.tablet_id_ = sys_tablet_id;
    sys_tablet_key.reorganization_scn_ = reorgan_scn;
    sys_tablet_key.meta_scn_ = sys_tablet_meta_scn;
    sys_tablet_meta_id = sys_tablet_key.convert_to_macro_id(false/*is_in_table*/);
    ASSERT_TRUE(sys_tablet_meta_id.is_valid());

    ASSERT_EQ(OB_SUCCESS, sys_tiered_store.list_meta_macro_files(
        sys_tablet_id.is_ls_inner_tablet(), sys_ls_id.id(), sys_tablet_id.id(),
        reorgan_scn.get_val_for_inner_table_field(), sys_sub_meta_ids));
    ASSERT_GE(sys_sub_meta_ids.count(), 1);
  }

  // Prepare user-tenant objects to verify that the SYS LS checkpoint excludes other tenants.
  ObTabletID tenant_tablet_id;
  ObLSID tenant_ls_id;
  share::SCN tenant_tablet_meta_scn;
  blocksstable::MacroBlockId tenant_tablet_meta_id;
  ObArray<blocksstable::MacroBlockId> tenant_sub_meta_ids;
  {
    share::ObTenantSwitchGuard tenant_guard;
    OK(tenant_guard.switch_to(run_ctx_.tenant_id_));

    ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
    ASSERT_NE(nullptr, meta_svr);

    ObSqlString tenant_table_name;
    ObSqlString create_tenant_table_sql;
    ASSERT_EQ(OB_SUCCESS, tenant_table_name.assign_fmt("tenant_ls_ckpt_tbl_%ld", table_suffix));
    ASSERT_EQ(OB_SUCCESS, create_tenant_table_sql.assign_fmt(
        "create table %s (a int)", tenant_table_name.ptr()));
    ASSERT_EQ(OB_SUCCESS, exe_sql(create_tenant_table_sql.ptr()));
    ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(tenant_table_name.ptr(),
                                               tenant_tablet_id, tenant_ls_id));
    ASSERT_TRUE(tenant_tablet_id.is_valid());
    ASSERT_TRUE(tenant_ls_id.is_valid());
    LOG_INFO("got tenant tablet_id and ls_id", K(tenant_tablet_id), K(tenant_ls_id));

    ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(tenant_ls_id, tenant_tablet_id,
                                                  reorgan_scn, true/*check_lease*/));
    ObArenaAllocator tablet_alloc("LsCkptTntTab");
    ObTabletHandle tablet_hdl;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(tenant_ls_id, tenant_tablet_id,
                                                reorgan_scn, tablet_alloc,
                                                tablet_hdl, tenant_tablet_meta_scn));
    ASSERT_TRUE(tablet_hdl.is_valid());
    ASSERT_TRUE(tenant_tablet_meta_scn.is_valid());

    ObSSMetaTabletKey tenant_tablet_key;
    tenant_tablet_key.ls_id_ = tenant_ls_id;
    tenant_tablet_key.tablet_id_ = tenant_tablet_id;
    tenant_tablet_key.reorganization_scn_ = reorgan_scn;
    tenant_tablet_key.meta_scn_ = tenant_tablet_meta_scn;
    tenant_tablet_meta_id = tenant_tablet_key.convert_to_macro_id(false/*is_in_table*/);
    ASSERT_TRUE(tenant_tablet_meta_id.is_valid());

    ObTieredMetadataStore tenant_tiered_store;
    ASSERT_EQ(OB_SUCCESS, tenant_tiered_store.init(run_ctx_.tenant_id_, GCTX.sql_proxy_));
    ASSERT_EQ(OB_SUCCESS, tenant_tiered_store.list_meta_macro_files(
        tenant_tablet_id.is_ls_inner_tablet(), tenant_ls_id.id(), tenant_tablet_id.id(),
        reorgan_scn.get_val_for_inner_table_field(), tenant_sub_meta_ids));
    ASSERT_GE(tenant_sub_meta_ids.count(), 1);
  }

  // Pause background checkpoints so periodic tasks cannot overlap with the test DAG.
  {
    share::ObTenantSwitchGuard sys_guard;
    OK(sys_guard.switch_to(OB_SYS_TENANT_ID));

    ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
    ASSERT_NE(nullptr, meta_svr);
    ObSSLSMeta ls_meta;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(sys_ls_id, ls_meta));
    ASSERT_TRUE(ls_meta.is_valid());
    ASSERT_EQ(SYS_LS, sys_ls_id);

    ObSSLsCkptExecutor executor;
    ASSERT_EQ(OB_SUCCESS, executor.init());
    NamedEventGuard feature_guard;
    if (!GCONF.enable_logservice) {
      ASSERT_EQ(OB_SUCCESS, feature_guard.init_always_for_all_threads(
          "EN_SS_LS_CKPT_FORCE_ENABLE_WITHOUT_LOGSERVICE", OB_EAGAIN));
    }
    const bool saved_in_bootstrap = GCTX.in_bootstrap_;
    DEFER(GCTX.in_bootstrap_ = saved_in_bootstrap);
    GCTX.in_bootstrap_ = true;
    SysLsCkptPauseGuard ckpt_pause_guard;
    ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());
    DEFER(executor.cancel_checkpoint_dag_and_wait());
    // Wait for PALF publication when logservice is enabled;
    // otherwise, schedule the DAG and wait for its exit.
    if (GCONF.enable_logservice) {
      ASSERT_EQ(OB_SUCCESS, executor.do_checkpoint(ObSSLsCkptMode::BOOTSTRAP));
    } else {
      bool is_scheduled = false;
      ASSERT_EQ(OB_SUCCESS, executor.schedule_checkpoint_dag(
          ObSSLsCkptMode::BOOTSTRAP, is_scheduled));
      ASSERT_TRUE(is_scheduled);
      ASSERT_EQ(OB_SUCCESS, wait_ls_ckpt_write_dag_exit(executor));
    }

    // Locate the root object through the Reader after PALF publication,
    // or through Shared Meta otherwise.
    blocksstable::MacroBlockId ls_ckpt_id;
    if (GCONF.enable_logservice) {
      ObSSLsCkptReader ckpt_reader;
      ASSERT_EQ(OB_SUCCESS, ckpt_reader.init());
      ASSERT_EQ(OB_SUCCESS, ckpt_reader.load(SYS_LS));
      ls_ckpt_id = ckpt_reader.get_ckpt_macro_id();
    } else {
      share::SCN ckpt_meta_scn;
      ObSSMetaLSCkptKey ckpt_key;
      ASSERT_EQ(OB_SUCCESS, get_latest_ls_ckpt_scn(*meta_svr, SYS_LS, ckpt_meta_scn));
      ckpt_key.ls_id_ = sys_ls_id;
      ckpt_key.meta_scn_ = ckpt_meta_scn;
      ls_ckpt_id = ckpt_key.convert_to_macro_id(false/*is_in_table*/);
    }
    ASSERT_TRUE(ls_ckpt_id.is_valid());
    ASSERT_EQ(OB_SUCCESS, validate_ls_ckpt_object_id(SYS_LS, ls_ckpt_id));

    const uint64_t meta_scn_val = static_cast<uint64_t>(ls_ckpt_id.meta_scn());

    if (GCONF.enable_logservice) {
      share::SCN service_meta_scn;
      ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_ckpt_meta_scn(sys_ls_id, service_meta_scn));
      ASSERT_EQ(meta_scn_val, service_meta_scn.get_val_for_inner_table_field());
    }

    {
      ObSSMetaIterGuard<ObSSMetaLSCkptKeyIter> ckpt_iter_guard;
      ObSSMetaLSCkptKeyIter *ckpt_iter = nullptr;
      ObSSMetaLSCkptKey ckpt_key;
      bool found_ckpt_key = false;
      int tmp_ret = OB_SUCCESS;
      ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_ckpt_iter(
          sys_ls_id, share::SCN::max_scn(), false/*read_mark_delete*/, ckpt_iter_guard));
      ASSERT_EQ(OB_SUCCESS, ckpt_iter_guard.get_iter(ckpt_iter));
      while (OB_SUCCESS == (tmp_ret = ckpt_iter->get_next(ckpt_key))) {
        if (ckpt_key.ls_id_ == sys_ls_id
            && ckpt_key.meta_scn_.get_val_for_inner_table_field() == meta_scn_val) {
          found_ckpt_key = true;
          break;
        }
      }
      ASSERT_TRUE(found_ckpt_key);
    }

    bool exist = false;
    ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(ls_ckpt_id, 0/*ls_epoch*/, exist));
    ASSERT_TRUE(exist);

    // Read the root object directly from object storage
    // to verify that its ID points to readable content.
    ObArenaAllocator oss_alloc("LsCkptOss");
    ObString ls_ckpt_data_buf;
    ASSERT_EQ(OB_SUCCESS, read_oss_object(ls_ckpt_id, ObTabletID(), oss_alloc, ls_ckpt_data_buf));

    ObSSLsCkptData ckpt_data;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, ckpt_data.deserialize(ls_ckpt_data_buf.ptr(),
                                                ls_ckpt_data_buf.length(), pos));
    ASSERT_EQ(ls_ckpt_data_buf.length(), pos);
    ASSERT_TRUE(ckpt_data.is_valid());
    ASSERT_TRUE(ckpt_data.ls_meta_.is_valid());
    ASSERT_EQ(sys_ls_id, ckpt_data.ls_meta_.ls_id_);
    ASSERT_EQ(static_cast<uint64_t>(ls_ckpt_id.meta_scn()),
              ckpt_data.ckpt_scn_.get_val_for_inner_table_field());
    ASSERT_TRUE(ckpt_data.reserved_meta_scn_.is_valid());
    ASSERT_LE(ckpt_data.reserved_meta_scn_, ckpt_data.ckpt_scn_);
    for (int64_t i = 0; i < ckpt_data.tablet_meta_keys_.count(); ++i) {
      ASSERT_GE(ckpt_data.tablet_meta_keys_.at(i).meta_scn(), 0);
      ASSERT_LE(static_cast<uint64_t>(ckpt_data.tablet_meta_keys_.at(i).meta_scn()),
                ckpt_data.reserved_meta_scn_.get_val_for_inner_table_field());
    }

    // Rescan at this checkpoint's reserved_meta_scn and verify the root object's tablet order.
    ObSSMetaTabletKeyIterator ordered_tablet_iter;
    ASSERT_EQ(OB_SUCCESS, ordered_tablet_iter.init(
        *GCTX.sql_proxy_, OB_SYS_TENANT_ID, sys_ls_id,
        ckpt_data.reserved_meta_scn_, true /*skip_mark_delete*/));
    int64_t ordered_idx = 0;
    int ret = OB_SUCCESS;
    ObSSMetaTabletKey ordered_tablet_key;
    while (OB_SUCC(ordered_tablet_iter.get_next(ordered_tablet_key))) {
      ASSERT_LT(ordered_idx, ckpt_data.tablet_meta_keys_.count());
      const blocksstable::MacroBlockId expected_object_id =
          ordered_tablet_key.convert_to_macro_id(false /*is_in_table*/);
      ASSERT_TRUE(expected_object_id.is_valid());
      ASSERT_EQ(expected_object_id, ckpt_data.tablet_meta_keys_.at(ordered_idx));
      ++ordered_idx;
      ordered_tablet_key.reset();
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(ordered_idx, ckpt_data.tablet_meta_keys_.count());

    // Read through the public ObSSMetaService API
    // and compare the result with direct deserialization.
    ObSSLSMeta read_ls_meta;
    ObSEArray<blocksstable::MacroBlockId, 64> read_tablet_meta_keys;
    ASSERT_EQ(OB_SUCCESS, meta_svr->read_ls_ckpt_data(
        sys_ls_id, ckpt_data.ckpt_scn_, read_ls_meta, read_tablet_meta_keys));
    ASSERT_TRUE(read_ls_meta.is_valid());
    ASSERT_EQ(ckpt_data.ls_meta_.ls_id_, read_ls_meta.ls_id_);
    ASSERT_EQ(ckpt_data.tablet_meta_keys_.count(), read_tablet_meta_keys.count());
    for (int64_t i = 0; i < read_tablet_meta_keys.count(); ++i) {
      ASSERT_TRUE(ckpt_data.tablet_meta_keys_.at(i) == read_tablet_meta_keys.at(i));
    }

    // The root must include the SYS tablet,
    // and its tablet meta and all sub-meta must be readable from OSS.
    bool found_sys_tablet_meta = false;
    for (int64_t i = 0; i < ckpt_data.tablet_meta_keys_.count(); ++i) {
      if (ckpt_data.tablet_meta_keys_.at(i) == sys_tablet_meta_id) {
        found_sys_tablet_meta = true;
        break;
      }
    }
    ASSERT_TRUE(found_sys_tablet_meta);

    ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(
        sys_tablet_meta_id, 0/*ls_epoch*/, exist));
    ASSERT_TRUE(exist);
    ObString sys_tablet_meta_buf;
    ASSERT_EQ(OB_SUCCESS, read_oss_object(sys_tablet_meta_id, sys_tablet_id,
                                          oss_alloc, sys_tablet_meta_buf));
    ASSERT_GT(sys_tablet_meta_buf.length(), 0);

    for (int64_t i = 0; i < sys_sub_meta_ids.count(); ++i) {
      blocksstable::MacroBlockId sys_sub_meta_oss_id = to_oss_sub_meta_id(sys_sub_meta_ids.at(i));
      ASSERT_TRUE(sys_sub_meta_oss_id.is_shared_tablet_sub_meta());
      ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(
          sys_sub_meta_oss_id, 0/*ls_epoch*/, exist));
      ASSERT_TRUE(exist);
      ObString sys_sub_meta_buf;
      ASSERT_EQ(OB_SUCCESS, read_oss_object(sys_sub_meta_oss_id, sys_tablet_id,
                                            oss_alloc, sys_sub_meta_buf));
      ASSERT_GT(sys_sub_meta_buf.length(), 0);
    }

    executor.destroy();
  }

  // Check the object namespace from the user tenant's meta tenant.
  // Checking from SYS can report a false positive when tenant-local tablet IDs match.
  {
    share::ObTenantSwitchGuard meta_guard;
    OK(meta_guard.switch_to(gen_meta_tenant_id(run_ctx_.tenant_id_)));

    bool exist = false;
    ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(
        tenant_tablet_meta_id, 0/*ls_epoch*/, exist));
    ASSERT_FALSE(exist);
    for (int64_t i = 0; i < tenant_sub_meta_ids.count(); ++i) {
      blocksstable::MacroBlockId tenant_sub_meta_oss_id = to_oss_sub_meta_id(tenant_sub_meta_ids.at(i));
      ASSERT_TRUE(tenant_sub_meta_oss_id.is_shared_tablet_sub_meta());
      ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(
          tenant_sub_meta_oss_id, 0/*ls_epoch*/, exist));
      ASSERT_FALSE(exist);
    }
  }

  LOG_INFO("[TEST CASE END] sys_ls_ckpt_write_dag_uploads_sys_meta_to_oss");
}

// Perf: for a sys-tenant tablet, write many tablet_meta versions (same tablet,
// same reorganization_scn, increasing meta_scn) and measure get_tablet latency.
// get_tablet only needs the latest version; with the meta_key range + LIMIT 1
// push-down the server should hit a single row instead of scanning the whole
// version history, so latency stays flat regardless of version_count.
TEST_F(TestSharedMetaService, sys_get_tablet_latest_perf)
{
  LOG_INFO("[TEST CASE BEGIN] sys_get_tablet_latest_perf");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  INIT_SYS_TIERED_STORE_V2(tiered_store);

  // create a sys-tenant user table and locate its tablet
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("create database if not exists ss_meta_test"));
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(
      "create table if not exists ss_meta_test.sys_get_tablet_perf_tbl (a int)"));

  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_sys_tablet_and_ls_id("sys_get_tablet_perf_tbl", tablet_id, ls_id));
  ASSERT_TRUE(tablet_id.is_valid());
  ASSERT_TRUE(ls_id.is_valid());
  LOG_INFO("got sys tablet_id and ls_id", K(tablet_id), K(ls_id));

  // ensure create_tablet takes the real persist path (drop stale first-level rows
  // and sub_meta from previous runs), so the base version is freshly written.
  const share::SCN reorgan_scn = share::SCN::min_scn();
  CLEAN_SYS_TABLET_META(meta_svr, tiered_store, ls_id, tablet_id, reorgan_scn);
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id, tablet_id, reorgan_scn, true/*check_lease*/));

  // read the base version's full serialized tablet_meta value and its meta_scn.
  ObArenaAllocator allocator("GetTabPerf");
  ObTabletHandle base_hdl;
  share::SCN base_meta_scn;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(ls_id, tablet_id, reorgan_scn,
                                             allocator, base_hdl, base_meta_scn));
  ASSERT_TRUE(base_hdl.is_valid());
  ASSERT_TRUE(base_meta_scn.is_valid());

  ObSSMetaTabletKey base_key;
  base_key.ls_id_ = ls_id;
  base_key.tablet_id_ = tablet_id;
  base_key.reorganization_scn_ = reorgan_scn;
  base_key.meta_scn_ = base_meta_scn;
  ObSSMetaTabletRow base_row;
  ASSERT_EQ(OB_SUCCESS, meta_svr->read_tablet_meta(base_key, allocator, base_row));
  const ObString tablet_value = base_row.value_;  // lives in `allocator` for the whole case
  ASSERT_GT(tablet_value.length(), 0);

  // insert many extra versions reusing the same valid value, with increasing meta_scn.
  // (all versions share reorganization_scn, so the sub_meta in the tiered table is
  //  shared and the latest version stays fully readable.)
  const int64_t version_count = 100;
  ObSEArray<ObSSMetaTabletKey, 128> inserted_keys;
  for (int64_t i = 1; i <= version_count; ++i) {
    ObSSMetaTabletKey key;
    key.ls_id_ = ls_id;
    key.tablet_id_ = tablet_id;
    key.reorganization_scn_ = reorgan_scn;
    key.meta_scn_ = share::SCN::plus(base_meta_scn, i);
    ASSERT_TRUE(key.is_valid());
    ObSSMetaTabletRow row(static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META), key, tablet_value);
    ASSERT_EQ(OB_SUCCESS, meta_svr->insert_tablet_meta(row));
    ASSERT_EQ(OB_SUCCESS, inserted_keys.push_back(key));
  }
  const share::SCN latest_meta_scn = share::SCN::plus(base_meta_scn, version_count);
  LOG_INFO("inserted tablet versions", K(tablet_id), K(version_count),
           K(base_meta_scn), K(latest_meta_scn));

  // measure get_tablet (latest version) latency over several repeats.
  const int64_t repeat = 10;
  int64_t total_us = 0;
  int64_t max_us = 0;
  share::SCN got_meta_scn;
  for (int64_t i = 0; i < repeat; ++i) {
    ObArenaAllocator once_alloc("GetTabletOnce");
    ObTabletHandle hdl;
    share::SCN meta_scn;
    const int64_t start_us = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(ls_id, tablet_id, reorgan_scn,
                                               once_alloc, hdl, meta_scn));
    const int64_t cost_us = ObTimeUtility::current_time() - start_us;
    total_us += cost_us;
    if (cost_us > max_us) {
      max_us = cost_us;
    }
    ASSERT_TRUE(hdl.is_valid());
    ASSERT_TRUE(meta_scn.is_valid());
    got_meta_scn = meta_scn;
    LOG_INFO("get_tablet latest once", K(i), K(cost_us), K(meta_scn));
  }
  // get_tablet must return the newest version.
  ASSERT_EQ(latest_meta_scn, got_meta_scn);
  LOG_INFO("[PERF] sys_get_tablet_latest_perf", K(version_count), K(repeat),
           "avg_us", total_us / repeat, K(max_us), K(total_us), K(got_meta_scn));

  // cleanup inserted versions (base version + sub_meta are left for other cases /
  // TearDown residual cleaner to handle).
  for (int64_t i = 0; i < inserted_keys.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, delete_tablet_meta_row(*meta_svr, inserted_keys.at(i)));
  }
  LOG_INFO("[TEST CASE END] sys_get_tablet_latest_perf");
}

TEST_F(TestSharedMetaService, get_mini_minor_sstables_with_prewarm)
{
  LOG_INFO("[TEST CASE BEGIN] get_mini_minor_sstables_with_prewarm");
  const char *table_name = "test_ss_minor_meta_prefetch";
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(run_ctx_.tenant_id_));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table if exists test_ss_minor_meta_prefetch"));
  ASSERT_EQ(OB_SUCCESS, exe_sql("alter system set _shared_inc_sstable_upload_interval = '1s'"));
  ASSERT_EQ(OB_SUCCESS, exe_sql("alter system set inc_sstable_upload_thread_score = 20"));
  ASSERT_EQ(OB_SUCCESS, exe_sql("alter system set minor_compact_trigger = 2"));
  ASSERT_EQ(OB_SUCCESS, exe_sql("create table test_ss_minor_meta_prefetch (c1 int primary key)"));

  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(table_name, tablet_id, ls_id));
  ASSERT_EQ(OB_SUCCESS, exe_sql("insert into test_ss_minor_meta_prefetch values (1)"));
  ASSERT_EQ(OB_SUCCESS, exe_sql("alter system minor freeze"));

  ObArenaAllocator allocator("SSMinorPfw");
  ObTabletHandle ss_tablet_handle;
  bool has_mini_minor = false;
  int ret = OB_TABLET_NOT_EXIST;
  const int64_t timeout_us = 120 * 1000 * 1000L;
  const int64_t start_us = ObTimeUtility::current_time();
  while (!has_mini_minor && ObTimeUtility::current_time() - start_us < timeout_us) {
    ss_tablet_handle.reset();
    allocator.reuse();
    ret = meta_svr->get_tablet(
        ls_id, tablet_id, share::SCN::min_scn(), allocator, ss_tablet_handle);
    if (OB_SUCCESS == ret && ss_tablet_handle.is_valid()) {
      ObTableStoreIterator table_iter(false/*is_reverse*/, false/*need_load_sstable*/);
      ret = ss_tablet_handle.get_obj()->get_mini_minor_sstables(table_iter);
      has_mini_minor = OB_SUCCESS == ret && table_iter.count() > 0;
    } else if (OB_TABLET_NOT_EXIST != ret && OB_SUCCESS != ret) {
      break;
    }
    if (!has_mini_minor) {
      ob_usleep(100 * 1000);
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(has_mini_minor);
  ASSERT_TRUE(ss_tablet_handle.is_valid());
  ObTablet *ss_tablet = ss_tablet_handle.get_obj();
  ASSERT_NE(nullptr, ss_tablet);
  ASSERT_TRUE(ss_tablet->table_store_addr_.is_disk_object());

  ObMetaDiskAddr table_store_addr = ss_tablet->table_store_addr_.addr_;
  ObMetaDiskAddr sstable_addr;
  ObStorageMetaValue::MetaType sstable_meta_type = ObStorageMetaValue::MAX;
  {
    ObTableStoreIterator table_iter(false/*is_reverse*/, false/*need_load_sstable*/);
    ObITable *table = nullptr;
    ASSERT_EQ(OB_SUCCESS, ss_tablet->get_mini_minor_sstables(table_iter));
    ASSERT_EQ(OB_SUCCESS, table_iter.get_next(table));
    ASSERT_NE(nullptr, table);
    ASSERT_TRUE(table->is_sstable());
    blocksstable::ObSSTable *sstable = static_cast<blocksstable::ObSSTable *>(table);
    ASSERT_TRUE(sstable->get_addr().is_block());
    sstable_addr = sstable->get_addr();
    sstable_meta_type = sstable->is_co_sstable()
        ? ObStorageMetaValue::CO_SSTABLE
        : ObStorageMetaValue::SSTABLE;
  }

  blocksstable::MacroBlockId table_store_block_id;
  blocksstable::MacroBlockId sstable_block_id;
  ASSERT_EQ(OB_SUCCESS, table_store_addr.get_macro_block_id(table_store_block_id));
  ASSERT_EQ(OB_SUCCESS, sstable_addr.get_macro_block_id(sstable_block_id));
  ASSERT_TRUE(table_store_block_id.is_shared_tablet_sub_meta_in_table());
  ASSERT_TRUE(sstable_block_id.is_shared_tablet_sub_meta_in_table());

  ObStorageMetaCache &meta_cache = OB_STORE_CACHE.get_storage_meta_cache();
  const ObStorageMetaKey table_store_key(MTL_ID(), table_store_addr);
  const ObStorageMetaKey sstable_key(MTL_ID(), sstable_addr);
  ASSERT_EQ(OB_SUCCESS, meta_cache.erase(table_store_key));
  ASSERT_EQ(OB_SUCCESS, meta_cache.erase(sstable_key));
  {
    ObStorageMetaHandle cache_handle;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, meta_cache.get_meta_without_prefetch(
        ObStorageMetaValue::TABLE_STORE, table_store_key, cache_handle));
    cache_handle.reset();
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, meta_cache.get_meta_without_prefetch(
        sstable_meta_type, sstable_key, cache_handle));
  }

  {
    // Do not let the iterator load SSTable meta one by one.  A cache hit after
    // this call can only come from the table-store range prewarm path.
    ObTableStoreIterator table_iter(false/*is_reverse*/, false/*need_load_sstable*/);
    ASSERT_EQ(OB_SUCCESS, ss_tablet->get_mini_minor_sstables_with_prewarm(table_iter));
    ASSERT_GT(table_iter.count(), 0);
  }

  {
    ObStorageMetaHandle cache_handle;
    ASSERT_EQ(OB_SUCCESS, meta_cache.get_meta_without_prefetch(
        sstable_meta_type, sstable_key, cache_handle));
  }

  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table if exists test_ss_minor_meta_prefetch"));
  LOG_INFO("[TEST CASE END] get_mini_minor_sstables_with_prewarm");
}

// ====================== Utility interfaces ======================

TEST_F(TestSharedMetaService, get_max_committed_meta_scn_basic)
{
  LOG_INFO("[TEST CASE BEGIN] get_max_committed_meta_scn_basic");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  share::SCN scn;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_max_committed_meta_scn(scn));
  ASSERT_TRUE(scn.is_valid());
  LOG_INFO("get_max_committed_meta_scn succeed", K(scn));
  LOG_INFO("[TEST CASE END] get_max_committed_meta_scn_basic");
}

TEST_F(TestSharedMetaService, active_fork_merge_update_reason_gate)
{
  LOG_INFO("[TEST CASE BEGIN] active_fork_merge_update_reason_gate");
  const char *table_name = "test_active_fork_merge_gate";
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(run_ctx_.tenant_id_));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table if exists test_active_fork_merge_gate"));
  ASSERT_EQ(OB_SUCCESS, exe_sql("create table test_active_fork_merge_gate (c1 int primary key)"));

  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(table_name, tablet_id, ls_id));
  const share::SCN reorgan_scn = share::SCN::min_scn();
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id, tablet_id, reorgan_scn, true /*check_lease*/));

  ObArenaAllocator allocator("ForkMergeGate");
  ObTabletHandle ss_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(ls_id, tablet_id, reorgan_scn, allocator, ss_tablet_handle));
  ASSERT_TRUE(ss_tablet_handle.is_valid());
  ObTablet *ss_tablet = ss_tablet_handle.get_obj();
  ASSERT_NE(nullptr, ss_tablet);

  share::ObForkTabletInfo &fork_info = ss_tablet->tablet_meta_.fork_info_;
  fork_info.set_src_tablet_id(ObTabletID(tablet_id.id() + 1));
  fork_info.set_fork_snapshot_version(1);
  ASSERT_TRUE(fork_info.is_active());

  bool can_generate_new_tablet = false;
  bool can_gc_sstable = false;
  ASSERT_EQ(OB_SUCCESS, compaction::ObSSTabletMergeHelper::check_can_generate_new_tablet(ls_id, ss_tablet_handle,
      ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MINOR_SSTABLE, true /*need_check_split*/, can_generate_new_tablet,
      can_gc_sstable));
  ASSERT_TRUE(can_generate_new_tablet);
  ASSERT_FALSE(can_gc_sstable);

  ASSERT_EQ(OB_SUCCESS, compaction::ObSSTabletMergeHelper::check_can_generate_new_tablet(ls_id, ss_tablet_handle,
      ObMetaUpdateReason::TABLET_UPLOAD_COMPACT_ADD_DATA_MINOR_SSTABLE, true /*need_check_split*/,
      can_generate_new_tablet, can_gc_sstable));
  ASSERT_TRUE(can_generate_new_tablet);
  ASSERT_FALSE(can_gc_sstable);

  ASSERT_EQ(OB_SUCCESS, compaction::ObSSTabletMergeHelper::check_can_generate_new_tablet(ls_id, ss_tablet_handle,
      ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE, true /*need_check_split*/, can_generate_new_tablet,
      can_gc_sstable));
  ASSERT_FALSE(can_generate_new_tablet);
  ASSERT_FALSE(can_gc_sstable);

  ASSERT_EQ(OB_SUCCESS, compaction::ObSSTabletMergeHelper::check_can_generate_new_tablet(ls_id, ss_tablet_handle,
      ObMetaUpdateReason::TABLET_UPLOAD_REPLACE_MINOR_SSTABLE, true /*need_check_split*/, can_generate_new_tablet,
      can_gc_sstable));
  ASSERT_FALSE(can_generate_new_tablet);
  ASSERT_FALSE(can_gc_sstable);

  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table if exists test_active_fork_merge_gate"));
  LOG_INFO("[TEST CASE END] active_fork_merge_update_reason_gate");
}

TEST_F(TestSharedMetaService, add_diagnose_info_basic)
{
  LOG_INFO("[TEST CASE BEGIN] add_diagnose_info_basic");
  uint64_t tenant_id = run_ctx_.tenant_id_;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(tenant_id));

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  // add_diagnose_info is purely in-memory, just verify it doesn't crash
  meta_svr->add_diagnose_info("test_func", "test_reason", 42);
  LOG_INFO("add_diagnose_info succeed");
  LOG_INFO("[TEST CASE END] add_diagnose_info_basic");
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
  const int ret = GCONF.add_extra_config(
      oceanbase::unittest::SYS_CKPT_STARTUP_OPTION);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to set startup sys checkpoint thread score: %d\n", ret);
    return ret;
  }

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
