// owner: yichang.yyf
// owner group: transaction

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX TABLELOCK
#define protected public
#define private public

#include "storage/ls/ob_ls.h"
#include "logservice/ob_log_service.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "mtlenv/tablelock/table_lock_common_env.h"
#include "unittest/storage/test_tablet_helper.h"
#include "lib/alloc/alloc_func.h"
#include "storage/init_basic_struct.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tx/ob_trans_part_ctx.h"

#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace checkpoint;
}
namespace transaction
{
namespace tablelock
{
namespace
{
const int64_t TEST_TENANT_MEMORY_LIMIT = 8L * 1024 * 1024 * 1024;
const int64_t TEST_LOG_DISK_USAGE_LIMIT = 20L * 1024 * 1024 * 1024;
int64_t previous_tenant_memory_limit = 0;
}

class TestLockMemtableCheckpoint : public ::testing::Test
{
public:
  TestLockMemtableCheckpoint()
    : memtable_(nullptr),
      fake_t3m_(common::OB_SERVER_TENANT_ID),
      ls_id_(next_ls_id_()),
      ls_(nullptr),
      ls_handle_(),
      ls_created_(false)
  {
    LOG_INFO("construct TestLockMemtableCheckpoint", K(ls_id_));
  }
  ~TestLockMemtableCheckpoint() = default;

  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp() override
  {
    ObCreateLSArg arg;
    ObLSService *ls_svr = MTL(ObLSService*);
    logservice::ObLogService *log_service = MTL(logservice::ObLogService *);

    ASSERT_NE(nullptr, ls_svr);
    ASSERT_NE(nullptr, log_service);
    ASSERT_EQ(OB_SUCCESS,
              log_service->update_log_disk_usage_limit_size(TEST_LOG_DISK_USAGE_LIMIT));
    ASSERT_EQ(OB_SUCCESS,
              storage::gen_create_ls_arg(OB_SYS_TENANT_ID, ls_id_, arg));
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->create_ls(arg));
    ls_created_ = true;
    ASSERT_EQ(OB_SUCCESS,
              ls_svr->get_ls(ls_id_, ls_handle_, ObLSGetMod::TABLELOCK_MOD));
    ASSERT_NE(nullptr, ls_ = ls_handle_.get_ls());
    ASSERT_EQ(OB_SUCCESS, ls_->get_lock_table()->get_lock_memtable(table_handle_));
    ASSERT_EQ(OB_SUCCESS, table_handle_.get_lock_memtable(memtable_));
    LOG_INFO("set up success");
  }
  void TearDown() override
  {
    EXPECT_EQ(OB_SUCCESS, cleanup_ls_());
    LOG_INFO("tear down success", K(ls_id_));
  }

protected:
  static ObLSID next_ls_id_()
  {
    static int64_t next_ls_id = 100;
    return ObLSID(next_ls_id++);
  }

  static share::SCN make_test_scn_(const int64_t value)
  {
    share::SCN scn;
    scn.set_base();
    scn.val_ = value;
    return scn;
  }

  static bool find_lock_op_status_(ObOBJLock *obj_lock,
                                   const ObTableLockOp &target,
                                   ObTableLockOpStatus &status)
  {
    bool found = false;
    for (int64_t i = 0; !found && i < TABLE_LOCK_MODE_COUNT; ++i) {
      ObTableLockOpList *op_list = obj_lock->map_[i];
      if (OB_NOT_NULL(op_list)) {
        DLIST_FOREACH_NORET(curr, *op_list) {
          if (!curr->lock_op_.need_replay_or_recover(target)) {
            status = curr->lock_op_.lock_op_status_;
            found = true;
            break;
          }
        }
      }
    }
    return found;
  }

  static int add_table_lock_mds_(ObPartTransCtx &tx_ctx,
                                 const ObTableLockOp &lock_op,
                                 char *buf,
                                 const int64_t buf_len,
                                 const bool durable = true)
  {
    int ret = OB_SUCCESS;
    ObTxBufferNode mds_node;
    if (OB_FAIL(build_table_lock_mds_node_(
            lock_op, buf, buf_len, mds_node))) {
      LOG_WARN("build table lock mds node failed", K(ret), K(lock_op));
    } else {
      if (durable) {
        mds_node.set_submitted();
        mds_node.set_synced();
      }
      if (OB_FAIL(tx_ctx.exec_info_.multi_data_source_.push_back(mds_node))) {
        LOG_WARN("add table lock mds failed", K(ret), K(lock_op));
      }
    }
    return ret;
  }

  static int add_cached_table_lock_mds_(ObPartTransCtx &tx_ctx,
                                        const ObTableLockOp &lock_op,
                                        char *buf,
                                        const int64_t buf_len)
  {
    int ret = OB_SUCCESS;
    ObTxBufferNode mds_node;
    if (OB_FAIL(build_table_lock_mds_node_(
            lock_op, buf, buf_len, mds_node))) {
      LOG_WARN("build cached table lock mds node failed", K(ret), K(lock_op));
    } else if (OB_FAIL(tx_ctx.mds_cache_.insert_mds_node(mds_node))) {
      LOG_WARN("add cached table lock mds failed", K(ret), K(lock_op));
    }
    return ret;
  }

  static int build_table_lock_mds_node_(const ObTableLockOp &lock_op,
                                        char *buf,
                                        const int64_t buf_len,
                                        ObTxBufferNode &mds_node)
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(lock_op.serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize table lock op failed", K(ret), K(lock_op));
    } else {
      const common::ObString mds_data(pos, buf);
      if (OB_FAIL(mds_node.init(ObTxDataSourceType::TABLE_LOCK,
                                mds_data,
                                SCN::min_scn(),
                                lock_op.lock_seq_no_,
                                nullptr))) {
        LOG_WARN("init table lock mds failed", K(ret), K(lock_op));
      }
    }
    return ret;
  }

  void assert_lock_op_status_(const ObTableLockOp &lock_op,
                              const ObTableLockOpStatus expected_status)
  {
    ObOBJLock *obj_lock = nullptr;
    ObTableLockOpStatus actual_status = UNKNOWN_STATUS;
    ASSERT_EQ(OB_SUCCESS,
              memtable_->obj_lock_map_.lock_map_.get(lock_op.lock_id_, obj_lock));
    ASSERT_NE(nullptr, obj_lock);
    const bool found = find_lock_op_status_(obj_lock, lock_op, actual_status);
    EXPECT_TRUE(found);
    if (found) {
      EXPECT_EQ(expected_status, actual_status);
    }
    memtable_->obj_lock_map_.lock_map_.revert(obj_lock);
  }

  void init_memtable_ctx_(memtable::ObMemtableCtx &mt_ctx)
  {
    ASSERT_EQ(OB_SUCCESS, mt_ctx.ctx_cb_allocator_.init(OB_SYS_TENANT_ID));
    ASSERT_EQ(OB_SUCCESS, mt_ctx.lock_mem_ctx_.init(table_handle_));
  }

  void destroy_memtable_ctx_(memtable::ObMemtableCtx &mt_ctx)
  {
    mt_ctx.lock_mem_ctx_.reset();
    mt_ctx.ctx_cb_allocator_.reset();
  }

  void verify_multiple_out_trans_mds_takeover_()
  {
    static const int64_t TABLE_LOCK_OP_COUNT = 5;
    const share::SCN start_working_scn = make_test_scn_(30);
    ObPartTransCtx tx_ctx;
    ObTableLockOp table_lock_ops[TABLE_LOCK_OP_COUNT];
    char mds_buf[TABLE_LOCK_OP_COUNT][ObTableLockOp::MAX_SERIALIZE_SIZE];

    tx_ctx.default_init_();
    init_memtable_ctx_(tx_ctx.mt_ctx_);
    tx_ctx.exec_info_.state_ = ObTxState::PREPARE;
    for (int64_t i = 0; i < TABLE_LOCK_OP_COUNT; ++i) {
      table_lock_ops[i] = TABLE_LOCK_OP_COUNT - 1 == i
                              ? DEFAULT_OUT_TRANS_UNLOCK_OP
                              : DEFAULT_OUT_TRANS_LOCK_OP;
      ASSERT_EQ(OB_SUCCESS, get_lock_id(1000 + i, table_lock_ops[i].lock_id_));
      table_lock_ops[i].lock_seq_no_ = ObTxSEQ(100 + i, 0);
      ASSERT_EQ(OB_SUCCESS,
                add_table_lock_mds_(
                    tx_ctx, table_lock_ops[i], mds_buf[i], sizeof(mds_buf[i])));
    }
    ASSERT_EQ(TABLE_LOCK_OP_COUNT, tx_ctx.exec_info_.multi_data_source_.count());

    // Any OUT_TRANS candidate requires the strict
    // start_working_scn > flushed_scn rule.
    memtable_->flushed_scn_ = start_working_scn;
    ASSERT_EQ(OB_ERR_UNEXPECTED,
              tx_ctx.recover_table_lock_mds_for_takeover_(start_working_scn));
    ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());

    memtable_->flushed_scn_ = make_test_scn_(10);
    ASSERT_EQ(OB_SUCCESS,
              tx_ctx.recover_table_lock_mds_for_takeover_(start_working_scn));
    ASSERT_EQ(TABLE_LOCK_OP_COUNT, tx_ctx.mt_ctx_.lock_mem_ctx_.get_lock_op_count());
    ASSERT_EQ(start_working_scn, tx_ctx.mt_ctx_.lock_mem_ctx_.max_durable_scn_);
    for (int64_t i = 0; i < TABLE_LOCK_OP_COUNT; ++i) {
      assert_lock_op_status_(table_lock_ops[i], LOCK_OP_DOING);
    }

    ASSERT_EQ(OB_SUCCESS,
              tx_ctx.mt_ctx_.lock_mem_ctx_.clear_table_lock(
                  false, SCN::min_scn(), make_test_scn_(40)));
    ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());
    tx_ctx.exec_info_.multi_data_source_.reset();
    destroy_memtable_ctx_(tx_ctx.mt_ctx_);
  }

  int cleanup_ls_()
  {
    int ret = OB_SUCCESS;
    table_handle_.reset();
    ls_handle_.reset();
    memtable_ = nullptr;
    ls_ = nullptr;
    if (ls_created_) {
      ObLSService *ls_svr = MTL(ObLSService *);
      if (OB_ISNULL(ls_svr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls service is null", K(ret));
      } else if (OB_FAIL(ls_svr->remove_ls(ls_id_))) {
        LOG_WARN("remove ls failed", K(ret), K(ls_id_));
      }
      if (OB_SUCC(ret)) {
        ls_created_ = false;
      }
    }
    return ret;
  }

  ObLockMemtable *memtable_;
  ObTableHandleV2 table_handle_;
  ObTenantMetaMemMgr fake_t3m_;
  ObFreezer freezer_;
  ObLSID ls_id_;
  ObLS *ls_;
  ObLSHandle ls_handle_;
  bool ls_created_;

  ObArenaAllocator allocator_;
};

void TestLockMemtableCheckpoint::SetUpTestCase()
{
  LOG_INFO("SetUpTestCase");
  init_default_lock_test_value();
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
  // Each fixture owns a distinct LS. Safe destruction is asynchronous, so
  // several removed LSs can coexist while this test suite is running.
  previous_tenant_memory_limit = lib::get_tenant_memory_limit(MTL_ID());
  lib::set_tenant_memory_limit(
      MTL_ID(), OB_MAX(previous_tenant_memory_limit, TEST_TENANT_MEMORY_LIMIT));
}

void TestLockMemtableCheckpoint::TearDownTestCase()
{
  LOG_INFO("TearDownTestCase");
  lib::set_tenant_memory_limit(MTL_ID(), previous_tenant_memory_limit);
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestLockMemtableCheckpoint, replay_disorder)
{
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder");
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  int ret = OB_SUCCESS;
  ObCreateLSArg arg;

  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.set_base();
  commit_scn.set_base();

  // 1.recover unlock op and lock op
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 1");
  ret = memtable_->recover_obj_lock(DEFAULT_OUT_TRANS_UNLOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = memtable_->recover_obj_lock(DEFAULT_OUT_TRANS_LOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2. update lock status disorder
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 2");
  commit_version.val_ = 3;
  commit_scn.val_ = 3;
  ret = memtable_->update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);

  commit_version.val_ = 2;
  commit_scn.val_ = 2;
  ret = memtable_->update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. check checkpoint
  // The rec_scn should be equal with the smaller commit_scn
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 3");
  ASSERT_EQ(commit_scn.val_, memtable_->get_rec_scn().val_);

  // 4. flush and get a previous commit log
  // You will find the log about disordered replay in the log file.
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 4");
  ret = memtable_->recover_obj_lock(DEFAULT_OUT_TRANS_UNLOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, memtable_->flush(share::SCN::max_scn(), 0));
  commit_version.val_ = 1;
  commit_scn.val_ = 1;
  ret = memtable_->update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 5. check checkpoint
  // The rec_scn should be equal with the smaller commit_scn
  // during flushing (i.e. it's get from pre_rec_scn)
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 5");
  ASSERT_EQ(commit_scn.val_, memtable_->get_rec_scn().val_);

  // 6. get a commit log with a commit_scn which
  // is larger than freeze_scn during flushing
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 6");
  ret = memtable_->recover_obj_lock(DEFAULT_OUT_TRANS_LOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  commit_version.val_ = 4;
  commit_scn.val_ = 4;
  ret = memtable_->update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);

  // 7. check checkpoint
  // The rec_scn should still be equal with the smaller
  // commit_scn during flushing (i.e. it's get from pre_rec_scn)
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 7");
  ASSERT_EQ(1, memtable_->get_rec_scn().val_);

  // 8. flush finish
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 8");
  ret = memtable_->on_memtable_flushed();
  ASSERT_EQ(OB_SUCCESS, ret);

  // 9. check checkpoint
  // The rec_scn should be equal with the latest commit_scn
  // which got during previous flushing (i.e. it's get from rec_scn)
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 9");
  ASSERT_EQ(commit_scn.val_, memtable_->get_rec_scn().val_);

}

TEST_F(TestLockMemtableCheckpoint, ignore_out_trans_doing_and_restore_durable_scn)
{
  // Recover a stale OUT_TRANS DOING snapshot whose durable SCN is behind the
  // lock-table flush. Verify that the op is ignored while its replay watermark
  // is retained.
  const share::SCN durable_scn = make_test_scn_(10);
  ObTableLockInfo table_lock_info;
  ObTableLockOp unlock_op = DEFAULT_OUT_TRANS_UNLOCK_OP;
  bool need_replay = true;

  table_lock_info.max_durable_scn_ = durable_scn;
  ASSERT_EQ(OB_SUCCESS, table_lock_info.table_lock_ops_.push_back(unlock_op));
  memtable_->flushed_scn_ = make_test_scn_(20);

  {
    memtable::ObMemtableCtx mt_ctx;
    init_memtable_ctx_(mt_ctx);
    ASSERT_EQ(OB_SUCCESS,
              mt_ctx.recover_from_table_lock_durable_info(table_lock_info));
    ASSERT_TRUE(mt_ctx.lock_mem_ctx_.lock_list_.is_empty());
    ASSERT_EQ(durable_scn, mt_ctx.lock_mem_ctx_.max_durable_scn_);

    ASSERT_EQ(OB_SUCCESS,
              mt_ctx.check_lock_need_replay(make_test_scn_(9), unlock_op, need_replay));
    ASSERT_FALSE(need_replay);
    ASSERT_EQ(OB_SUCCESS,
              mt_ctx.check_lock_need_replay(durable_scn, unlock_op, need_replay));
    ASSERT_TRUE(need_replay);
    destroy_memtable_ctx_(mt_ctx);
  }

  ObOBJLock *obj_lock = nullptr;
  const int ret = memtable_->obj_lock_map_.lock_map_.get(unlock_op.lock_id_, obj_lock);
  if (OB_SUCCESS == ret) {
    memtable_->obj_lock_map_.lock_map_.revert(obj_lock);
  }
  ASSERT_TRUE(OB_ENTRY_NOT_EXIST == ret || OB_OBJ_LOCK_NOT_EXIST == ret);
}

TEST_F(TestLockMemtableCheckpoint, recover_durable_table_lock_mds_before_takeover)
{
  // Start with a durable PREPARE transaction and an empty lock memtable.
  // Rebuild its OUT_TRANS DOING op at start-working, then verify idempotent
  // takeover retries and the final commit transition.
  const share::SCN durable_scn = make_test_scn_(20);
  const share::SCN start_working_scn = make_test_scn_(30);
  const share::SCN next_start_working_scn = make_test_scn_(35);
  const share::SCN commit_scn = make_test_scn_(40);
  ObTableLockOp lock_op = DEFAULT_OUT_TRANS_LOCK_OP;
  ObPartTransCtx tx_ctx;
  char mds_buf[ObTableLockOp::MAX_SERIALIZE_SIZE];

  tx_ctx.default_init_();
  init_memtable_ctx_(tx_ctx.mt_ctx_);
  tx_ctx.exec_info_.state_ = ObTxState::PREPARE;
  ASSERT_EQ(OB_SUCCESS, tx_ctx.mt_ctx_.lock_mem_ctx_.sync_log_succ(durable_scn));
  memtable_->flushed_scn_ = make_test_scn_(10);
  ASSERT_EQ(OB_SUCCESS,
            add_table_lock_mds_(tx_ctx, lock_op, mds_buf, sizeof(mds_buf)));

  ASSERT_EQ(OB_SUCCESS,
            tx_ctx.recover_table_lock_mds_for_takeover_(start_working_scn));
  ASSERT_FALSE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());
  ASSERT_EQ(1, tx_ctx.mt_ctx_.lock_mem_ctx_.get_lock_op_count());
  ASSERT_EQ(start_working_scn, tx_ctx.mt_ctx_.lock_mem_ctx_.max_durable_scn_);

  ObOBJLock *obj_lock = nullptr;
  ObTableLockOpStatus status = UNKNOWN_STATUS;
  ASSERT_EQ(OB_SUCCESS, memtable_->obj_lock_map_.lock_map_.get(lock_op.lock_id_, obj_lock));
  ASSERT_TRUE(find_lock_op_status_(obj_lock, lock_op, status));
  ASSERT_EQ(LOCK_OP_DOING, status);
  memtable_->obj_lock_map_.lock_map_.revert(obj_lock);

  // Retrying the same durable takeover fence must not duplicate the operation.
  ASSERT_EQ(OB_SUCCESS,
            tx_ctx.recover_table_lock_mds_for_takeover_(start_working_scn));
  ASSERT_EQ(1, tx_ctx.mt_ctx_.lock_mem_ctx_.get_lock_op_count());
  ASSERT_EQ(start_working_scn, tx_ctx.mt_ctx_.lock_mem_ctx_.max_durable_scn_);

  // A newer takeover fence must advance the watermark even when identity
  // filtering finds that the operation has already been reconstructed.
  ASSERT_EQ(OB_SUCCESS,
            tx_ctx.recover_table_lock_mds_for_takeover_(next_start_working_scn));
  ASSERT_EQ(1, tx_ctx.mt_ctx_.lock_mem_ctx_.get_lock_op_count());
  ASSERT_EQ(next_start_working_scn, tx_ctx.mt_ctx_.lock_mem_ctx_.max_durable_scn_);

  ASSERT_EQ(OB_SUCCESS,
            tx_ctx.mt_ctx_.lock_mem_ctx_.clear_table_lock(
                true, commit_scn, commit_scn));
  ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());
  ASSERT_EQ(OB_SUCCESS, memtable_->obj_lock_map_.lock_map_.get(lock_op.lock_id_, obj_lock));
  ASSERT_TRUE(find_lock_op_status_(obj_lock, lock_op, status));
  ASSERT_EQ(LOCK_OP_COMPLETE, status);
  memtable_->obj_lock_map_.lock_map_.revert(obj_lock);
  tx_ctx.exec_info_.multi_data_source_.reset();
  destroy_memtable_ctx_(tx_ctx.mt_ctx_);
}

TEST_F(TestLockMemtableCheckpoint, reject_cached_table_lock_mds_during_takeover)
{
  // Construct cache-only and durable-plus-cache TABLE_LOCK states that cannot
  // occur in a valid recovered committable transaction. Takeover must reject
  // both without attaching an operation.
  ObPartTransCtx tx_ctx;
  ObTableLockOp lock_op = DEFAULT_OUT_TRANS_LOCK_OP;
  char durable_mds_buf[ObTableLockOp::MAX_SERIALIZE_SIZE];
  char cached_mds_buf[ObTableLockOp::MAX_SERIALIZE_SIZE];

  tx_ctx.default_init_();
  init_memtable_ctx_(tx_ctx.mt_ctx_);
  tx_ctx.exec_info_.state_ = ObTxState::PREPARE;
  memtable_->flushed_scn_ = make_test_scn_(10);

  // Cache-only TABLE_LOCK MDS means the durable recovery source is missing.
  ASSERT_EQ(OB_SUCCESS,
            add_cached_table_lock_mds_(
                tx_ctx, lock_op, cached_mds_buf, sizeof(cached_mds_buf)));
  ASSERT_EQ(0, tx_ctx.exec_info_.multi_data_source_.count());
  ASSERT_TRUE(tx_ctx.mds_cache_.is_contain(ObTxDataSourceType::TABLE_LOCK));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            tx_ctx.recover_table_lock_mds_for_takeover_(make_test_scn_(20)));
  ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());

  tx_ctx.mds_cache_.reset();
  // A durable node with a cached node violates the MDS callback ordering.
  ASSERT_EQ(OB_SUCCESS,
            add_table_lock_mds_(
                tx_ctx, lock_op, durable_mds_buf, sizeof(durable_mds_buf)));
  ASSERT_EQ(OB_SUCCESS,
            add_cached_table_lock_mds_(
                tx_ctx, lock_op, cached_mds_buf, sizeof(cached_mds_buf)));
  ASSERT_EQ(1, tx_ctx.exec_info_.multi_data_source_.count());
  ASSERT_TRUE(tx_ctx.mds_cache_.is_contain(ObTxDataSourceType::TABLE_LOCK));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            tx_ctx.recover_table_lock_mds_for_takeover_(make_test_scn_(20)));
  ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());

  tx_ctx.mds_cache_.reset();
  tx_ctx.exec_info_.multi_data_source_.reset();
  destroy_memtable_ctx_(tx_ctx.mt_ctx_);
}

TEST_F(TestLockMemtableCheckpoint, validate_table_lock_mds_takeover_state)
{
  // Exercise the transaction-state and replay-SCN gates around takeover.
  // Non-committable states skip reconstruction, while malformed durability or
  // stale replay fences fail before mutating the lock context. Multiple
  // durable OUT_TRANS nodes also verify that every candidate is reconstructed.
  ObPartTransCtx tx_ctx;
  ObTableLockOp lock_op = DEFAULT_OUT_TRANS_LOCK_OP;
  char mds_buf[ObTableLockOp::MAX_SERIALIZE_SIZE];

  ASSERT_FALSE(ObPartTransCtx::can_recover_table_lock_mds_for_takeover_(
      ObTxState::INIT));
  ASSERT_TRUE(ObPartTransCtx::can_recover_table_lock_mds_for_takeover_(
      ObTxState::REDO_COMPLETE));
  ASSERT_TRUE(ObPartTransCtx::can_recover_table_lock_mds_for_takeover_(
      ObTxState::PREPARE));
  ASSERT_TRUE(ObPartTransCtx::can_recover_table_lock_mds_for_takeover_(
      ObTxState::PRE_COMMIT));
  ASSERT_FALSE(ObPartTransCtx::can_recover_table_lock_mds_for_takeover_(
      ObTxState::COMMIT));
  ASSERT_FALSE(ObPartTransCtx::can_recover_table_lock_mds_for_takeover_(
      ObTxState::ABORT));
  ASSERT_FALSE(ObPartTransCtx::can_recover_table_lock_mds_for_takeover_(
      ObTxState::CLEAR));

  tx_ctx.default_init_();
  init_memtable_ctx_(tx_ctx.mt_ctx_);
  tx_ctx.exec_info_.state_ = ObTxState::INIT;
  memtable_->flushed_scn_ = make_test_scn_(10);
  ASSERT_EQ(OB_SUCCESS,
            add_table_lock_mds_(tx_ctx, lock_op, mds_buf, sizeof(mds_buf)));
  ASSERT_EQ(OB_SUCCESS,
            tx_ctx.recover_table_lock_mds_for_takeover_(make_test_scn_(20)));
  ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());

  ObOBJLock *obj_lock = nullptr;
  const int ret = memtable_->obj_lock_map_.lock_map_.get(lock_op.lock_id_, obj_lock);
  if (OB_SUCCESS == ret) {
    memtable_->obj_lock_map_.lock_map_.revert(obj_lock);
  }
  ASSERT_TRUE(OB_ENTRY_NOT_EXIST == ret || OB_OBJ_LOCK_NOT_EXIST == ret);

  tx_ctx.exec_info_.state_ = ObTxState::ABORT;
  ASSERT_EQ(OB_SUCCESS,
            tx_ctx.recover_table_lock_mds_for_takeover_(make_test_scn_(20)));
  ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());
  tx_ctx.exec_info_.state_ = ObTxState::CLEAR;
  ASSERT_EQ(OB_SUCCESS,
            tx_ctx.recover_table_lock_mds_for_takeover_(make_test_scn_(20)));
  ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());

  tx_ctx.exec_info_.multi_data_source_.reset();
  tx_ctx.exec_info_.state_ = ObTxState::PREPARE;
  ASSERT_EQ(OB_SUCCESS,
            add_table_lock_mds_(
                tx_ctx, lock_op, mds_buf, sizeof(mds_buf), false /*durable*/));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            tx_ctx.recover_table_lock_mds_for_takeover_(make_test_scn_(20)));
  ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());

  tx_ctx.exec_info_.multi_data_source_.reset();
  const share::SCN durable_scn = make_test_scn_(20);
  ASSERT_EQ(OB_SUCCESS, tx_ctx.mt_ctx_.lock_mem_ctx_.sync_log_succ(durable_scn));
  ASSERT_EQ(OB_SUCCESS,
            add_table_lock_mds_(tx_ctx, lock_op, mds_buf, sizeof(mds_buf)));

  memtable_->flushed_scn_ = make_test_scn_(30);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            tx_ctx.recover_table_lock_mds_for_takeover_(make_test_scn_(30)));
  ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());

  memtable_->flushed_scn_ = make_test_scn_(5);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            tx_ctx.recover_table_lock_mds_for_takeover_(make_test_scn_(19)));
  ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());

  // Equality with max_durable_scn_ is valid, while the OUT_TRANS replay SCN
  // remains strictly newer than the lock memtable flushed_scn_.
  ASSERT_EQ(OB_SUCCESS,
            tx_ctx.recover_table_lock_mds_for_takeover_(durable_scn));
  ASSERT_EQ(1, tx_ctx.mt_ctx_.lock_mem_ctx_.get_lock_op_count());
  ASSERT_EQ(OB_SUCCESS,
            tx_ctx.mt_ctx_.lock_mem_ctx_.clear_table_lock(
                false, SCN::min_scn(), make_test_scn_(21)));

  tx_ctx.exec_info_.multi_data_source_.reset();
  destroy_memtable_ctx_(tx_ctx.mt_ctx_);

  // Verify multiple durable TABLE_LOCK nodes without creating another fixture
  // LS. IN_TRANS operations are intentionally absent because they are not
  // registered as TABLE_LOCK MDS.
  verify_multiple_out_trans_mds_takeover_();
}

TEST_F(TestLockMemtableCheckpoint, preserve_complete_lock_when_stale_doing_is_skipped)
{
  // Seed the lock table with a durable COMPLETE operation, then recover an
  // older tx ctx DOING snapshot for the same identity. The stale snapshot must
  // not replace or remove the terminal lock-table state.
  const share::SCN commit_scn = make_test_scn_(10);
  ObTableLockOp lock_op = DEFAULT_OUT_TRANS_LOCK_OP;
  ObTableLockOp complete_lock_op = lock_op;
  ObTableLockInfo table_lock_info;
  ObOBJLock *obj_lock = nullptr;
  ObTableLockOpStatus status = UNKNOWN_STATUS;

  complete_lock_op.lock_op_status_ = LOCK_OP_COMPLETE;
  complete_lock_op.commit_version_ = commit_scn;
  complete_lock_op.commit_scn_ = commit_scn;
  ASSERT_EQ(OB_SUCCESS, memtable_->recover_obj_lock(complete_lock_op));
  memtable_->flushed_scn_ = make_test_scn_(20);
  table_lock_info.max_durable_scn_ = commit_scn;
  ASSERT_EQ(OB_SUCCESS, table_lock_info.table_lock_ops_.push_back(lock_op));

  {
    memtable::ObMemtableCtx mt_ctx;
    init_memtable_ctx_(mt_ctx);
    ASSERT_EQ(OB_SUCCESS, mt_ctx.recover_from_table_lock_durable_info(table_lock_info));
    ASSERT_TRUE(mt_ctx.lock_mem_ctx_.lock_list_.is_empty());
    destroy_memtable_ctx_(mt_ctx);
  }

  ASSERT_EQ(OB_SUCCESS, memtable_->obj_lock_map_.lock_map_.get(lock_op.lock_id_, obj_lock));
  ASSERT_TRUE(find_lock_op_status_(obj_lock, lock_op, status));
  ASSERT_EQ(LOCK_OP_COMPLETE, status);
  memtable_->obj_lock_map_.lock_map_.revert(obj_lock);
}

TEST_F(TestLockMemtableCheckpoint, discard_stale_out_trans_unlock_after_flushed_commit)
{
  const share::SCN commit_scn = make_test_scn_(10);
  const share::SCN flushed_scn = make_test_scn_(20);
  ObTableLockOp unlock_op = DEFAULT_OUT_TRANS_UNLOCK_OP;
  ObTableLockOp complete_unlock_op = unlock_op;
  ObTableLockInfo table_lock_info;
  ObPartTransCtx tx_ctx;
  ObOBJLock *obj_lock = nullptr;
  ObTableLockOpStatus status = UNKNOWN_STATUS;

  // Recreate the restart input of the original issue: the lock table has
  // already persisted the terminal unlock, while an older tx ctx table
  // snapshot still carries the corresponding DOING operation.
  complete_unlock_op.lock_op_status_ = LOCK_OP_COMPLETE;
  complete_unlock_op.commit_version_ = commit_scn;
  complete_unlock_op.commit_scn_ = commit_scn;
  ASSERT_EQ(OB_SUCCESS, memtable_->recover_obj_lock(complete_unlock_op));
  memtable_->flushed_scn_ = flushed_scn;
  ASSERT_GE(memtable_->flushed_scn_, commit_scn);

  table_lock_info.max_durable_scn_ = commit_scn;
  ASSERT_EQ(OB_SUCCESS, table_lock_info.table_lock_ops_.push_back(unlock_op));

  tx_ctx.default_init_();
  init_memtable_ctx_(tx_ctx.mt_ctx_);
  ASSERT_EQ(OB_SUCCESS,
            tx_ctx.mt_ctx_.recover_from_table_lock_durable_info(table_lock_info));
  ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());

  char mds_buf[ObTableLockOp::MAX_SERIALIZE_SIZE];
  ASSERT_EQ(OB_SUCCESS,
            add_table_lock_mds_(tx_ctx, unlock_op, mds_buf, sizeof(mds_buf)));

  // A terminal transaction must not reconstruct TABLE_LOCK MDS during
  // takeover. The lock table sstable remains authoritative.
  tx_ctx.exec_info_.state_ = ObTxState::COMMIT;
  ASSERT_EQ(OB_SUCCESS,
            tx_ctx.recover_table_lock_mds_for_takeover_(make_test_scn_(30)));
  ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());

  ASSERT_EQ(OB_SUCCESS,
            memtable_->obj_lock_map_.lock_map_.get(unlock_op.lock_id_, obj_lock));
  ASSERT_TRUE(find_lock_op_status_(obj_lock, unlock_op, status));
  ASSERT_EQ(LOCK_OP_COMPLETE, status);
  memtable_->obj_lock_map_.lock_map_.revert(obj_lock);

  tx_ctx.exec_info_.multi_data_source_.reset();
  destroy_memtable_ctx_(tx_ctx.mt_ctx_);
}

TEST_F(TestLockMemtableCheckpoint, rebuild_out_trans_doing_at_final_scn)
{
  // Recover only the replay watermark from a stale tx ctx snapshot, then
  // replay TABLE_LOCK MDS at the equal final SCN. Equality must remain
  // replayable so the operation can transition to COMPLETE.
  const share::SCN commit_scn = make_test_scn_(10);
  ObTableLockOp lock_op = DEFAULT_OUT_TRANS_LOCK_OP;
  ObTableLockInfo table_lock_info;
  ObOBJLock *obj_lock = nullptr;
  ObTableLockOpStatus status = UNKNOWN_STATUS;

  table_lock_info.max_durable_scn_ = commit_scn;
  ASSERT_EQ(OB_SUCCESS, table_lock_info.table_lock_ops_.push_back(lock_op));
  memtable_->flushed_scn_ = make_test_scn_(5);

  {
    memtable::ObMemtableCtx mt_ctx;
    init_memtable_ctx_(mt_ctx);
    ASSERT_EQ(OB_SUCCESS, mt_ctx.recover_from_table_lock_durable_info(table_lock_info));
    ASSERT_TRUE(mt_ctx.lock_mem_ctx_.lock_list_.is_empty());

    // TABLE_LOCK MDS replays at the final SCN. Equality with max_durable_scn_
    // must not be filtered, because the replay guard uses a strict comparison.
    ASSERT_EQ(OB_SUCCESS, memtable_->replay_lock(&mt_ctx, lock_op, commit_scn));
    ASSERT_EQ(OB_SUCCESS,
              memtable_->update_lock_status(lock_op,
                                             commit_scn,
                                             commit_scn,
                                             LOCK_OP_COMPLETE));
    ASSERT_FALSE(mt_ctx.lock_mem_ctx_.lock_list_.is_empty());
    destroy_memtable_ctx_(mt_ctx);
  }

  ASSERT_EQ(OB_SUCCESS, memtable_->obj_lock_map_.lock_map_.get(lock_op.lock_id_, obj_lock));
  ASSERT_TRUE(find_lock_op_status_(obj_lock, lock_op, status));
  ASSERT_EQ(LOCK_OP_COMPLETE, status);
  memtable_->obj_lock_map_.lock_map_.revert(obj_lock);
}

TEST_F(TestLockMemtableCheckpoint, remove_rebuilt_out_trans_doing_on_abort)
{
  // Rebuild an unresolved PREPARE transaction's OUT_TRANS DOING operation at
  // start-working. An eventual abort must remove it from both the transaction
  // lock context and the lock memtable.
  const share::SCN start_working_scn = make_test_scn_(10);
  const share::SCN abort_scn = make_test_scn_(20);
  ObTableLockOp lock_op = DEFAULT_OUT_TRANS_LOCK_OP;
  ObPartTransCtx tx_ctx;
  ObOBJLock *obj_lock = nullptr;
  ObTableLockOpStatus status = UNKNOWN_STATUS;
  char mds_buf[ObTableLockOp::MAX_SERIALIZE_SIZE];

  memtable_->flushed_scn_ = make_test_scn_(5);
  tx_ctx.default_init_();
  init_memtable_ctx_(tx_ctx.mt_ctx_);
  tx_ctx.exec_info_.state_ = ObTxState::PREPARE;
  ASSERT_EQ(OB_SUCCESS,
            add_table_lock_mds_(tx_ctx, lock_op, mds_buf, sizeof(mds_buf)));
  ASSERT_EQ(OB_SUCCESS,
            tx_ctx.recover_table_lock_mds_for_takeover_(start_working_scn));
  ASSERT_FALSE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());

  ASSERT_EQ(OB_SUCCESS,
            tx_ctx.mt_ctx_.lock_mem_ctx_.clear_table_lock(
                false, SCN::min_scn(), abort_scn));
  ASSERT_TRUE(tx_ctx.mt_ctx_.lock_mem_ctx_.lock_list_.is_empty());

  const int ret = memtable_->obj_lock_map_.lock_map_.get(lock_op.lock_id_, obj_lock);
  if (OB_SUCCESS == ret) {
    ASSERT_FALSE(find_lock_op_status_(obj_lock, lock_op, status));
    memtable_->obj_lock_map_.lock_map_.revert(obj_lock);
  } else {
    ASSERT_TRUE(OB_ENTRY_NOT_EXIST == ret || OB_OBJ_LOCK_NOT_EXIST == ret);
  }
  tx_ctx.exec_info_.multi_data_source_.reset();
  destroy_memtable_ctx_(tx_ctx.mt_ctx_);
}

TEST_F(TestLockMemtableCheckpoint, preserve_doing_lock_for_transfer_and_in_trans_recovery)
{
  // Compare the exceptional recovery paths that still attach DOING state:
  // transfer merge preserves OUT_TRANS operations, while normal tx ctx
  // recovery continues to restore IN_TRANS operations.
  ObTableLockInfo transfer_lock_info;
  ObTableLockInfo in_trans_lock_info;
  ObTableLockOp out_trans_lock_op = DEFAULT_OUT_TRANS_LOCK_OP;
  ObTableLockOp in_trans_lock_op = DEFAULT_IN_TRANS_LOCK_OP;

  transfer_lock_info.max_durable_scn_ = make_test_scn_(10);
  ASSERT_EQ(OB_SUCCESS, transfer_lock_info.table_lock_ops_.push_back(out_trans_lock_op));
  {
    memtable::ObMemtableCtx mt_ctx;
    init_memtable_ctx_(mt_ctx);
    ASSERT_EQ(OB_SUCCESS,
              mt_ctx.recover_from_table_lock_durable_info(transfer_lock_info, true));
    ASSERT_FALSE(mt_ctx.lock_mem_ctx_.lock_list_.is_empty());
    assert_lock_op_status_(out_trans_lock_op, LOCK_OP_DOING);
    destroy_memtable_ctx_(mt_ctx);
  }

  in_trans_lock_info.max_durable_scn_ = make_test_scn_(20);
  ASSERT_EQ(OB_SUCCESS, in_trans_lock_info.table_lock_ops_.push_back(in_trans_lock_op));
  {
    memtable::ObMemtableCtx mt_ctx;
    ObOBJLock *obj_lock = nullptr;
    init_memtable_ctx_(mt_ctx);
    ASSERT_EQ(OB_SUCCESS, mt_ctx.recover_from_table_lock_durable_info(in_trans_lock_info));
    ASSERT_FALSE(mt_ctx.lock_mem_ctx_.lock_list_.is_empty());
    // ROW_EXCLUSIVE IN_TRANS_DML_LOCK is represented by a counter rather than
    // an operation node in ObOBJLock::map_.
    ASSERT_FALSE(in_trans_lock_op.need_record_lock_op());
    ASSERT_EQ(OB_SUCCESS,
              memtable_->obj_lock_map_.lock_map_.get(in_trans_lock_op.lock_id_, obj_lock));
    ASSERT_EQ(1, obj_lock->row_exclusive_);
    memtable_->obj_lock_map_.lock_map_.revert(obj_lock);
    destroy_memtable_ctx_(mt_ctx);
  }
}
}  // namespace tablelock
}  // namespace transaction
}  // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_lock_memtable_checkpoint.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
