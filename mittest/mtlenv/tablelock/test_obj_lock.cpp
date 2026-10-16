// owner: cxf262476
// owner group: transaction

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX TABLELOCK

#include <gtest/gtest.h>
#define protected public
#define private public
#define UNITTEST

#include "mtlenv/mock_tenant_module_env.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "table_lock_common_env.h"
#include "table_lock_tx_common_env.h"

namespace oceanbase
{

namespace storage
{
void ObTxData::dec_ref()
{
  return;
}
} // namespace storage

namespace transaction
{
namespace tablelock
{
class TestObjLock : public MockTxEnv,
                    public ::testing::Test
{
public:
  TestObjLock()
    : MockTxEnv(MTL_ID(), ObLSID(1)),
      fake_t3m_(common::OB_SERVER_TENANT_ID),
      obj_lock_(ObLockID())
  {}
  ~TestObjLock() {}

  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp() override
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
    // mock sequence no
    ObClockGenerator::init();
    create_memtable();
    LOG_INFO("set up success");
  }
  virtual void TearDown() override
  {
    ObClockGenerator::destroy();
    LOG_INFO("tear down success");
  }
public:
  void create_memtable()
  {
    ObITable::TableKey table_key;
    table_key.table_type_ = ObITable::LOCK_MEMTABLE;
    table_key.tablet_id_ = LS_LOCK_TABLET;
    table_key.scn_range_.start_scn_.convert_for_gts(1); // fake
    table_key.scn_range_.end_scn_.convert_for_gts(2); // fake

    ASSERT_EQ(OB_SUCCESS, memtable_.init(table_key, ls_id_, &freezer_));
    ASSERT_EQ(OB_SUCCESS, handle_.set_table(&memtable_, &fake_t3m_, ObITable::LOCK_MEMTABLE));
  }

  void start_tx(const ObTransID &tx_id, MyTxCtx &my_ctx)
  {
    MockTxEnv::start_tx(tx_id, handle_, my_ctx);
  }
  void get_store_ctx(MyTxCtx &my_ctx, ObStoreCtx &store_ctx)
  {
    MockTxEnv::get_store_ctx(my_ctx, &tx_table_, store_ctx);
  }
private:
  ObLockMemtable memtable_;
  ObTableHandleV2 handle_;
  storage::ObTenantMetaMemMgr fake_t3m_;
  ObFreezer freezer_;
  ObTxTable tx_table_;

private:
  ObOBJLock obj_lock_;
  ObMalloc allocator_;
};

void TestObjLock::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  init_default_lock_test_value();
}

void TestObjLock::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestObjLock, in_trans_lock)
{
  LOG_INFO("TestObjLock::in_trans_lock");
  // TEST SET
  // 1. IN TRANS LOCK LOCK
  // 2. IN TRANS LOCK COMMIT/ABORT
  int ret = OB_SUCCESS;
  ObLockParam param;
  bool is_try_lock = true;
  int64_t expired_time = 0;
  ObTxIDSet conflict_tx_set;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  // int64_t expired_time = ObClockGenerator::getClock() + 1000;


  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;
  // 1.1 try lock
  LOG_INFO("TestObjLock::in_trans_lock 1.1");
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);

  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = obj_lock_.lock(param,
                       store_ctx,
                       DEFAULT_IN_TRANS_LOCK_OP,
                       lock_mode_cnt_in_same_trans,
                       allocator_,
                       conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(obj_lock_.row_exclusive_, 1);
  // 1.2 lock and wait
  LOG_INFO("TestObjLock::in_trans_lock 1.2");
  is_try_lock = false;
  expired_time = 0;
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = obj_lock_.lock(param,
                       store_ctx,
                       DEFAULT_IN_TRANS_LOCK_OP,
                       lock_mode_cnt_in_same_trans,
                       allocator_,
                       conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(obj_lock_.row_exclusive_, 2);

  is_try_lock = false;
  expired_time = ObClockGenerator::getClock() + 1000;
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = obj_lock_.lock(param,
                       store_ctx,
                       DEFAULT_IN_TRANS_LOCK_OP,
                       lock_mode_cnt_in_same_trans,
                       allocator_,
                       conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(obj_lock_.row_exclusive_, 3);

  // 2.1 remove lock
  LOG_INFO("TestObjLock::in_trans_lock 2.1");
  for(int i = 3; i >0; i--) {
    obj_lock_.remove_lock_op(DEFAULT_IN_TRANS_LOCK_OP,
                             allocator_);
    ASSERT_EQ(obj_lock_.row_exclusive_, i - 1);
  }
}

TEST_F(TestObjLock, out_trans_lock)
{
  LOG_INFO("TestObjLock::out_trans_lock");
  // 1. OUT TRANS LOCK LOCK
  // 2. OUT TRANS LOCK COMMIT
  // 3. OUT TRANS LOCK UNLOCK
  // 4. OUT TRANS LOCK ABORT
  // 5. OUT TRANS LOCK RECOVER
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = 0;
  share::SCN min_commited_scn;
  share::SCN flushed_scn;
  ObTxIDSet conflict_tx_set;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  ObLockParam param;

  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;

  min_commited_scn.set_min();
  flushed_scn.set_min();
  // 1.1 try lock
  LOG_INFO("TestObjLock::out_trans_lock 1.1");
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);

  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = obj_lock_.lock(param,
                       store_ctx,
                       DEFAULT_OUT_TRANS_LOCK_OP,
                       lock_mode_cnt_in_same_trans,
                       allocator_,
                       conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  // 2.1 update lock status
  LOG_INFO("TestObjLock::out_trans_lock 2.1");
  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.set_base();
  commit_scn.set_base();
  ret = obj_lock_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS,
                                     allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  // 3.1 unlock
  LOG_INFO("TestObjLock::out_trans_lock 3.1");
  ret = obj_lock_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time,
                         allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  // 3.2 unlock commit
  LOG_INFO("TestObjLock::out_trans_lock 3.2");
  ret = obj_lock_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS,
                                     allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  // 4.1 lock
  LOG_INFO("TestObjLock::out_trans_lock 4.1");
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = obj_lock_.lock(param,
                       store_ctx,
                       DEFAULT_OUT_TRANS_LOCK_OP,
                       lock_mode_cnt_in_same_trans,
                       allocator_,
                       conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 4.2 abort
  LOG_INFO("TestObjLock::out_trans_lock 4.2");
  obj_lock_.remove_lock_op(DEFAULT_OUT_TRANS_LOCK_OP,
                           allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 4.3 check exist
  LOG_INFO("TestObjLock::out_trans_lock 4.3");
  ret = obj_lock_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS,
                                     allocator_);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  // 5.1 recover
  LOG_INFO("TestObjLock::out_trans_lock 5.1");
  ret = obj_lock_.recover_lock(DEFAULT_OUT_TRANS_LOCK_OP,
                               allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 5.2 check
  LOG_INFO("TestObjLock::out_trans_lock 5.2");
  ret = obj_lock_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS,
                                     allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  // 5.3 remove
  LOG_INFO("TestObjLock::out_trans_lock 5.3");
  obj_lock_.remove_lock_op(DEFAULT_OUT_TRANS_LOCK_OP,
                           allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestObjLock, out_trans_unlock_twice)
{
  LOG_INFO("TestObjLock::out_trans_lock");
  // TEST SET
  // 1. UNLOCK AFTER A COMMITTED UNLOCK
  // 2. UNLOCK AFTER A REPLAY AND COMMITTED UNLOCK
  // 3. UNLCOK AFTER A REPLAY AND UNCOMMITTED UNLOCK
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = 0;
  share::SCN min_commited_scn;
  share::SCN flushed_scn;
  share::SCN commit_version;
  share::SCN commit_scn;
  ObTxIDSet conflict_tx_set;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  ObLockParam param;

  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;

  min_commited_scn.set_min();
  flushed_scn.set_min();
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);

  // 1. UNLOCK AFTER A COMMON UNLOCK
  // The complete progress is:
  // lock -> lock commit -> unlock -> unlock commit -> unlock
  // The second unlock will throw an error due to there's no
  // paired lock.
  // 1.1 lock
  LOG_INFO("TestObjLock::out_trans_unlock_twice 1.1");
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = obj_lock_.lock(param,
                       store_ctx,
                       DEFAULT_OUT_TRANS_LOCK_OP,
                       lock_mode_cnt_in_same_trans,
                       allocator_,
                       conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  // 1.2 lock commit
  LOG_INFO("TestObjLock::out_trans_unlock_twice 1.2");
  commit_version.set_base();
  commit_scn.set_base();
  ret = obj_lock_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS,
                                     allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  // 1.3 unlock
  LOG_INFO("TestObjLock::out_trans_unlock_twice 1.3");
  ret = obj_lock_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time,
                         allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  // 1.4 unlock commit
  LOG_INFO("TestObjLock::out_trans_unlock_twice 1.4");
  ret = obj_lock_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS,
                                     allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  // 1.5 try to unlock again
  LOG_INFO("TestObjLock::out_trans_unlock_twice 1.5");
  ret = obj_lock_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time,
                         allocator_);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());

  // 2. UNLOCK AFTER A REPLAY AND COMMITTED UNLOCK
  // The complete progress is:
  // replay unlock -> unlock commit -> replay lock -> lock commit -> unlock
  // The replay logic of unlocking doesn't check whether there's a paired lock,
  // so the first unlcok will be executed successfully. And the second unlock
  // will see the same unlock op which is before it, so it should return error.
  // 2.1 replay unlock
  LOG_INFO("TestObjLock::out_trans_unlock_twice 2.1");
  ret = obj_lock_.recover_lock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                               allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.2 unlock commit
  LOG_INFO("TestObjLock::out_trans_unlock_twice 2.2");
  commit_version.set_base();
  commit_scn.set_base();
  ret = obj_lock_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS,
                                     allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  // 2.3 replay lock
  LOG_INFO("TestObjLock::out_trans_unlock_twice 2.3");
  ret = obj_lock_.recover_lock(DEFAULT_OUT_TRANS_LOCK_OP,
                               allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.4 lock commit
  LOG_INFO("TestObjLock::out_trans_unlock_twice 2.4");
  commit_version.set_base();
  commit_scn.set_base();
  ret = obj_lock_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS,
                                     allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  // 2.5 unlock
  LOG_INFO("TestObjLock::out_trans_unlock_twice 2.5");
  ret = obj_lock_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time,
                         allocator_);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  // 2.6 unlock commit to compat
  LOG_INFO("TestObjLock::out_trans_unlock_twice 2.6");
  ret = obj_lock_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS,
                                     allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  // 2.7 check
  LOG_INFO("TestObjLock::out_trans_unlock_twice 2.7");
  ret = obj_lock_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS,
                                     allocator_);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());

  // 3. UNLOCK AFTER A REPLAY AND UNCOMMITTED UNLOCK
  // The complete progress is:
  // replay unlock -> replay lock -> lock commit -> unlock -> unlcok commit
  // The second unlock will be failed due to there's another unlock op
  // in progress, but the latest unlock commit will executed successfully,
  // because it can see and commit the first unlock.
  // 3.1 replay unlock
  LOG_INFO("TestObjLock::out_trans_unlock_twice 3.1");
  ret = obj_lock_.recover_lock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                               allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 3.2 replay lock
  LOG_INFO("TestObjLock::out_trans_unlock_twice 3.2");
  ret = obj_lock_.recover_lock(DEFAULT_OUT_TRANS_LOCK_OP,
                               allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 3.3 lock commit
  LOG_INFO("TestObjLock::out_trans_unlock_twice 3.3");
  commit_version.set_base();
  commit_scn.set_base();
  ret = obj_lock_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS,
                                     allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  // 3.4 unlock
  LOG_INFO("TestObjLock::out_trans_unlock_twice 3.4");
  ret = obj_lock_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time,
                         allocator_);
  ASSERT_EQ(OB_OBJ_UNLOCK_CONFLICT, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  // 3.5 unlock commit
  LOG_INFO("TestObjLock::out_trans_unlock_twice 3.5");
  ret = obj_lock_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS,
                                     allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  // 3.6 check lock exist
  LOG_INFO("TestObjLock::out_trans_lock 3.6");
  ret = obj_lock_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS,
                                     allocator_);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  min_commited_scn = obj_lock_.get_min_ddl_lock_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
}

TEST_F(TestObjLock, lock_conflict_in_in)
{
  // 1. IN TRANS VS IN TRANS
  // 2. IN TRNAS VS OUT TRANS
  // 3. OUT TRANS VS IN TRANS
  // 4. OUT TRANS VS OUT TRANS
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = 0;
  ObTxIDSet conflict_tx_set;
  int64_t conflict_modes;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  ObLockParam param;

  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;

  MyTxCtx ctx2;
  ObStoreCtx store_ctx2;

  // prepare store ctx
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);

  start_tx(TRANS_ID2, ctx2);
  get_store_ctx(ctx2, store_ctx2);
  // 1.1 try lock
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = obj_lock_.lock(param,
                       store_ctx,
                       DEFAULT_IN_TRANS_LOCK_OP,
                       lock_mode_cnt_in_same_trans,
                       allocator_,
                       conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = obj_lock_.lock(param,
                       store_ctx,
                       DEFAULT_CONFLICT_OUT_TRANS_LOCK_OP,
                       lock_mode_cnt_in_same_trans,
                       allocator_,
                       conflict_tx_set);
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, ret);
  // 1.2 lock timeout
  is_try_lock = false;
  expired_time = ObClockGenerator::getClock() + 1000;
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = obj_lock_.lock(param,
                       store_ctx,
                       DEFAULT_CONFLICT_OUT_TRANS_LOCK_OP,
                       lock_mode_cnt_in_same_trans,
                       allocator_,
                       conflict_tx_set);
  if (DEFAULT_CONFLICT_OUT_TRANS_LOCK_OP.is_dml_lock_op()) {
    ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, ret);
  } else {
    // deadlock detect will kill the trans
    // ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, ret);
    // ASSERT_EQ(OB_TRANS_KILLED, ret);
    ASSERT_EQ((ret == OB_TRANS_KILLED || ret == OB_TRY_LOCK_ROW_CONFLICT), true);
  }

  // 1.3 clean.
  obj_lock_.remove_lock_op(DEFAULT_IN_TRANS_LOCK_OP,
                           allocator_);

  // 1.4 in trans conflict
  ObTableLockOp lock_first = DEFAULT_IN_TRANS_LOCK_OP;
  ObTableLockOp lock_second = DEFAULT_IN_TRANS_LOCK_OP;
  bool is_conflict = false;
  is_try_lock = true;
  expired_time = 0;
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  lock_second.create_trans_id_ = TRANS_ID2;
  for (int first = 0; first < TABLE_LOCK_MODE_COUNT; first++) {
    // lock the first lock
    lock_first.lock_mode_ = get_lock_mode_by_index(first);
    printf("first lock index: %d, mode: %d\n", first, lock_first.lock_mode_);
    ret = obj_lock_.lock(param,
                         store_ctx,
                         lock_first,
                         lock_mode_cnt_in_same_trans,
                         allocator_,
                         conflict_tx_set);
    ASSERT_EQ(OB_SUCCESS, ret);
    // lock the second lock and check conflict
    for (int second = 0; second < TABLE_LOCK_MODE_COUNT; second++) {
      lock_second.lock_mode_ = get_lock_mode_by_index(second);
      printf("second lock index: %d, mode: %d\n", second, lock_second.lock_mode_);
      is_conflict = !request_lock(lock_first.lock_mode_,
                                  lock_second.lock_mode_,
                                  conflict_modes);
      ret = obj_lock_.lock(param,
                           store_ctx2,
                           lock_second,
                           lock_mode_cnt_in_same_trans,
                           allocator_,
                           conflict_tx_set);
      if (is_conflict) {
        ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, ret);
      } else {
        ASSERT_EQ(OB_SUCCESS, ret);
        obj_lock_.remove_lock_op(lock_second,
                                 allocator_);
      }
    }
    // release the first lock
    obj_lock_.remove_lock_op(lock_first,
                             allocator_);
  }

}

TEST_F(TestObjLock, lock_conflict_in_out)
{
  // 1. IN TRANS VS IN TRANS
  // 2. IN TRNAS VS OUT TRANS
  // 3. OUT TRANS VS IN TRANS
  // 4. OUT TRANS VS OUT TRANS
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = 0;
  ObTxIDSet conflict_tx_set;
  int64_t conflict_modes = 0;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  ObLockParam param;

  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;

  MyTxCtx ctx2;
  ObStoreCtx store_ctx2;

  // prepare store ctx
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);

  start_tx(TRANS_ID2, ctx2);
  get_store_ctx(ctx2, store_ctx2);
  // 2.1 in trans conflict with out trans
  ObTableLockOp lock_first = DEFAULT_IN_TRANS_LOCK_OP;
  ObTableLockOp lock_second = DEFAULT_OUT_TRANS_LOCK_OP;
  bool is_conflict = false;
  is_try_lock = true;
  expired_time = 0;
  lock_second.lock_id_ = DEFAULT_TABLET_LOCK_ID;
  lock_second.create_trans_id_ = TRANS_ID2;
  lock_second.owner_id_ = OWNER_ID2;
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  for (int first = 0; first < TABLE_LOCK_MODE_COUNT; first++) {
    // lock the first lock
    lock_first.lock_mode_ = get_lock_mode_by_index(first);
    printf("first lock index: %d, mode: %d\n", first, lock_first.lock_mode_);
    ret = obj_lock_.lock(param,
                         store_ctx,
                         lock_first,
                         lock_mode_cnt_in_same_trans,
                         allocator_,
                         conflict_tx_set);
    ASSERT_EQ(OB_SUCCESS, ret);
    // lock the second lock and check conflict
    for (int second = 0; second < TABLE_LOCK_MODE_COUNT; second++) {
      lock_second.lock_mode_ = get_lock_mode_by_index(second);
      printf("second lock index: %d, mode: %d\n", second, lock_second.lock_mode_);
      is_conflict = !request_lock(lock_first.lock_mode_,
                                  lock_second.lock_mode_,
                                  conflict_modes);
      ret = obj_lock_.lock(param,
                           store_ctx2,
                           lock_second,
                           lock_mode_cnt_in_same_trans,
                           allocator_,
                           conflict_tx_set);
      if (is_conflict) {
        ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, ret);
      } else {
        ASSERT_EQ(OB_SUCCESS, ret);
        obj_lock_.remove_lock_op(lock_second,
                                 allocator_);
      }
    }
    // release the first lock
    obj_lock_.remove_lock_op(lock_first,
                             allocator_);
  }
}

TEST_F(TestObjLock, lock_conflict_out_in)
{
  // 1. IN TRANS VS IN TRANS
  // 2. IN TRNAS VS OUT TRANS
  // 3. OUT TRANS VS IN TRANS
  // 4. OUT TRANS VS OUT TRANS
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = 0;
  ObTxIDSet conflict_tx_set;
  int64_t conflict_modes = 0;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  ObLockParam param;

  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;

  MyTxCtx ctx2;
  ObStoreCtx store_ctx2;

  // prepare store ctx
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);

  start_tx(TRANS_ID2, ctx2);
  get_store_ctx(ctx2, store_ctx2);

  // 3.1 in trans conflict with out trans
  ObTableLockOp lock_first = DEFAULT_OUT_TRANS_LOCK_OP;
  ObTableLockOp lock_second = DEFAULT_IN_TRANS_LOCK_OP;
  bool is_conflict = false;
  lock_first.lock_id_ = DEFAULT_TABLET_LOCK_ID;
  lock_first.create_trans_id_ = TRANS_ID2;
  lock_first.owner_id_ = OWNER_ID2;
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  for (int first = 0; first < TABLE_LOCK_MODE_COUNT; first++) {
    // lock the first lock
    lock_first.lock_mode_ = get_lock_mode_by_index(first);
    printf("first lock index: %d, mode: %d\n", first, lock_first.lock_mode_);
    ret = obj_lock_.lock(param,
                         store_ctx,
                         lock_first,
                         lock_mode_cnt_in_same_trans,
                         allocator_,
                         conflict_tx_set);
    ASSERT_EQ(OB_SUCCESS, ret);
    // lock the second lock and check conflict
    for (int second = 0; second < TABLE_LOCK_MODE_COUNT; second++) {
      lock_second.lock_mode_ = get_lock_mode_by_index(second);
      printf("second lock index: %d, mode: %d\n", second, lock_second.lock_mode_);
      is_conflict = !request_lock(lock_first.lock_mode_,
                                  lock_second.lock_mode_,
                                  conflict_modes);
      ret = obj_lock_.lock(param,
                           store_ctx2,
                           lock_second,
                           lock_mode_cnt_in_same_trans,
                           allocator_,
                           conflict_tx_set);
      if (is_conflict) {
        ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, ret);
      } else {
        ASSERT_EQ(OB_SUCCESS, ret);
        obj_lock_.remove_lock_op(lock_second,
                                 allocator_);
      }
    }
    // release the first lock
    obj_lock_.remove_lock_op(lock_first,
                             allocator_);
  }
}

TEST_F(TestObjLock, lock_conflict_out_out)
{
  // 1. IN TRANS VS IN TRANS
  // 2. IN TRNAS VS OUT TRANS
  // 3. OUT TRANS VS IN TRANS
  // 4. OUT TRANS VS OUT TRANS
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = 0;
  ObTxIDSet conflict_tx_set;
  int64_t conflict_modes = 0;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  ObLockParam param;

  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;

  MyTxCtx ctx2;
  ObStoreCtx store_ctx2;

  // prepare store ctx
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);

  start_tx(TRANS_ID2, ctx2);
  get_store_ctx(ctx2, store_ctx2);

  // 4.1 out trans conflict with out trans
  ObTableLockOp lock_first = DEFAULT_OUT_TRANS_LOCK_OP;
  ObTableLockOp lock_second = DEFAULT_OUT_TRANS_LOCK_OP;
  bool is_conflict = false;
  lock_second.create_trans_id_ = TRANS_ID2;
  lock_first.owner_id_ = OWNER_ID2;
  lock_second.owner_id_ = OWNER_ID3;
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  for (int first = 0; first < TABLE_LOCK_MODE_COUNT; first++) {
    // lock the first lock
    lock_first.lock_mode_ = get_lock_mode_by_index(first);
    printf("first lock index: %d, mode: %d\n", first, lock_first.lock_mode_);
    ret = obj_lock_.lock(param,
                         store_ctx,
                         lock_first,
                         lock_mode_cnt_in_same_trans,
                         allocator_,
                         conflict_tx_set);
    ASSERT_EQ(OB_SUCCESS, ret);
    // lock the second lock and check conflict
    for (int second = 0; second < TABLE_LOCK_MODE_COUNT; second++) {
      lock_second.lock_mode_ = get_lock_mode_by_index(second);
      printf("second lock index: %d, mode: %d\n", second, lock_second.lock_mode_);
      is_conflict = !request_lock(lock_first.lock_mode_,
                                  lock_second.lock_mode_,
                                  conflict_modes);
      ret = obj_lock_.lock(param,
                           store_ctx2,
                           lock_second,
                           lock_mode_cnt_in_same_trans,
                           allocator_,
                           conflict_tx_set);
      if (is_conflict) {
        ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, ret);
      } else {
        ASSERT_EQ(OB_SUCCESS, ret);
        obj_lock_.remove_lock_op(lock_second,
                                 allocator_);
      }
    }
    // release the first lock
    obj_lock_.remove_lock_op(lock_first,
                             allocator_);
  }
}

TEST_F(TestObjLock, priority_conflict_outputs_head_tx)
{
  int ret = OB_SUCCESS;
  ObLockParam param;
  ObTxIDSet conflict_tx_set;
  ObTransID priority_conflict_tx;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};

  MyTxCtx ctx1;
  ObStoreCtx store_ctx1;
  MyTxCtx ctx2;
  ObStoreCtx store_ctx2;
  start_tx(DEFAULT_TRANS_ID, ctx1);
  get_store_ctx(ctx1, store_ctx1);
  start_tx(TRANS_ID2, ctx2);
  get_store_ctx(ctx2, store_ctx2);

  param.is_try_lock_ = false;
  param.is_two_phase_lock_ = true;
  param.lock_priority_ = ObTableLockPriority::NORMAL;
  param.expired_time_ = ObClockGenerator::getClock() + 1 * 1000 * 1000;

  ObTableLockOp first_op = DEFAULT_OUT_TRANS_LOCK_OP;
  ObTableLockOp second_op = DEFAULT_OUT_TRANS_LOCK_OP;
  second_op.create_trans_id_ = TRANS_ID2;
  second_op.owner_id_ = OWNER_ID2;

  ASSERT_EQ(OB_SUCCESS, obj_lock_.add_priority_task(param, store_ctx1, first_op, allocator_));
  ASSERT_EQ(OB_SUCCESS, obj_lock_.add_priority_task(param, store_ctx2, second_op, allocator_));

  ret = obj_lock_.lock(param,
                      store_ctx2,
                      second_op,
                      lock_mode_cnt_in_same_trans,
                      allocator_,
                      conflict_tx_set,
                      &priority_conflict_tx);
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, ret);
  ASSERT_EQ(0, conflict_tx_set.count());
  ASSERT_EQ(first_op.create_trans_id_, priority_conflict_tx);

  const ObTableLockPrioArg arg(param.lock_priority_);
  ASSERT_EQ(OB_SUCCESS, obj_lock_.remove_priority_task(arg, first_op, allocator_));
  ASSERT_EQ(OB_SUCCESS, obj_lock_.remove_priority_task(arg, second_op, allocator_));
}

TEST_F(TestObjLock, table_lock_continuation_param_is_limited_to_tablet_modes)
{
  ObLockParam param;
  ASSERT_EQ(OB_SUCCESS,
            param.set(DEFAULT_TABLET_LOCK_ID,
                      SHARE,
                      ObTableLockOwnerID::default_owner(),
                      OUT_TRANS_LOCK,
                      0));
  param.is_table_lock_continuation_ = true;
  ASSERT_TRUE(param.is_valid());
  param.lock_mode_ = SHARE_ROW_EXCLUSIVE;
  ASSERT_TRUE(param.is_valid());
  param.lock_mode_ = EXCLUSIVE;
  ASSERT_TRUE(param.is_valid());

  param.is_two_phase_lock_ = true;
  ASSERT_TRUE(param.is_valid());
  param.lock_priority_ = ObTableLockPriority::HIGH1;
  ASSERT_TRUE(param.is_valid());
  param.lock_priority_ = ObTableLockPriority::NORMAL;
  param.lock_mode_ = ROW_SHARE;
  ASSERT_FALSE(param.is_valid());
  param.lock_mode_ = ROW_EXCLUSIVE;
  ASSERT_FALSE(param.is_valid());
}

TEST_F(TestObjLock, table_lock_continuation_bypasses_high_priority_head)
{
  int ret = OB_SUCCESS;
  ObTxIDSet conflict_tx_set;
  ObTransID priority_conflict_tx;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};

  MyTxCtx ctx_s;
  MyTxCtx ctx_x;
  MyTxCtx ctx_later;
  ObStoreCtx store_ctx_s;
  ObStoreCtx store_ctx_x;
  ObStoreCtx store_ctx_later;
  start_tx(DEFAULT_TRANS_ID, ctx_s);
  start_tx(TRANS_ID2, ctx_x);
  start_tx(TRANS_ID3, ctx_later);
  get_store_ctx(ctx_s, store_ctx_s);
  get_store_ctx(ctx_x, store_ctx_x);
  get_store_ctx(ctx_later, store_ctx_later);

  ObLockParam continuation_param;
  continuation_param.is_try_lock_ = true;
  continuation_param.lock_priority_ = ObTableLockPriority::NORMAL;
  continuation_param.is_table_lock_continuation_ = true;
  continuation_param.expired_time_ = ObClockGenerator::getClock() + 1 * 1000 * 1000;

  ObLockParam high_param = continuation_param;
  high_param.is_two_phase_lock_ = true;
  high_param.lock_priority_ = ObTableLockPriority::HIGH1;
  high_param.is_table_lock_continuation_ = false;

  ObLockParam later_param = continuation_param;
  later_param.is_table_lock_continuation_ = false;

  ObTableLockOp s_op = DEFAULT_IN_TRANS_LOCK_OP;
  ObTableLockOp x_op = DEFAULT_IN_TRANS_LOCK_OP;
  ObTableLockOp later_op = DEFAULT_IN_TRANS_LOCK_OP;
  s_op.lock_id_ = DEFAULT_TABLET_LOCK_ID;
  x_op.lock_id_ = DEFAULT_TABLET_LOCK_ID;
  x_op.lock_mode_ = EXCLUSIVE;
  later_op.lock_id_ = DEFAULT_TABLET_LOCK_ID;
  x_op.create_trans_id_ = TRANS_ID2;
  x_op.owner_id_ = OWNER_ID2;
  later_op.create_trans_id_ = TRANS_ID3;
  later_op.owner_id_ = OWNER_ID3;

  // This models an admitted normal full-table request. Exercise both the
  // original non-two-phase form and the MySQL two-phase form, where the
  // continuation must retain and then consume its own priority task.
  const ObTableLockPrioArg high_arg(high_param.lock_priority_);
  const bool is_two_phase_options[] = {false, true};
  const int64_t two_phase_option_count =
      static_cast<int64_t>(sizeof(is_two_phase_options) / sizeof(is_two_phase_options[0]));
  const ObTableLockMode tablet_lock_modes[] = {SHARE, SHARE_ROW_EXCLUSIVE, EXCLUSIVE};
  const int64_t tablet_lock_mode_count =
      static_cast<int64_t>(sizeof(tablet_lock_modes) / sizeof(tablet_lock_modes[0]));
  for (int64_t phase = 0; phase < two_phase_option_count; ++phase) {
    continuation_param.is_two_phase_lock_ = is_two_phase_options[phase];
    for (int64_t i = 0; i < tablet_lock_mode_count; ++i) {
      s_op.lock_mode_ = tablet_lock_modes[i];
      later_op.lock_mode_ = tablet_lock_modes[i];
      conflict_tx_set.reset();
      priority_conflict_tx.reset();

      if (continuation_param.is_two_phase_lock_) {
        ASSERT_EQ(OB_SUCCESS,
                  obj_lock_.add_priority_task(continuation_param, store_ctx_s, s_op, allocator_));
      }
      ASSERT_EQ(OB_SUCCESS, obj_lock_.add_priority_task(high_param, store_ctx_x, x_op, allocator_));
      ASSERT_EQ(OB_SUCCESS, obj_lock_.lock(continuation_param,
                                           store_ctx_s,
                                           s_op,
                                           lock_mode_cnt_in_same_trans,
                                           allocator_,
                                           conflict_tx_set,
                                           &priority_conflict_tx));
      ASSERT_EQ(0, conflict_tx_set.count());

      // The exception is only for the admitted continuation. A later normal
      // tablet request is still ordered behind the HIGH1 queue head.
      obj_lock_.remove_lock_op(s_op, allocator_);
      conflict_tx_set.reset();
      priority_conflict_tx.reset();
      ret = obj_lock_.lock(later_param,
                           store_ctx_later,
                           later_op,
                           lock_mode_cnt_in_same_trans,
                           allocator_,
                           conflict_tx_set,
                           &priority_conflict_tx);
      ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, ret);
      ASSERT_EQ(0, conflict_tx_set.count());
      ASSERT_EQ(x_op.create_trans_id_, priority_conflict_tx);
      ASSERT_EQ(OB_SUCCESS, obj_lock_.remove_priority_task(high_arg, x_op, allocator_));
    }
  }
}

TEST_F(TestObjLock, table_lock_continuation_bypasses_normal_head)
{
  int ret = OB_SUCCESS;
  ObTxIDSet conflict_tx_set;
  ObTransID priority_conflict_tx;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};

  MyTxCtx ctx_s;
  MyTxCtx ctx_normal;
  ObStoreCtx store_ctx_s;
  ObStoreCtx store_ctx_normal;
  start_tx(DEFAULT_TRANS_ID, ctx_s);
  start_tx(TRANS_ID2, ctx_normal);
  get_store_ctx(ctx_s, store_ctx_s);
  get_store_ctx(ctx_normal, store_ctx_normal);

  ObLockParam continuation_param;
  continuation_param.is_try_lock_ = true;
  continuation_param.lock_priority_ = ObTableLockPriority::NORMAL;
  continuation_param.is_table_lock_continuation_ = true;
  continuation_param.expired_time_ = ObClockGenerator::getClock() + 1 * 1000 * 1000;

  ObLockParam queued_normal_param = continuation_param;
  queued_normal_param.is_two_phase_lock_ = true;
  queued_normal_param.is_table_lock_continuation_ = false;

  ObTableLockOp s_op = DEFAULT_IN_TRANS_LOCK_OP;
  ObTableLockOp queued_normal_op = DEFAULT_IN_TRANS_LOCK_OP;
  s_op.lock_id_ = DEFAULT_TABLET_LOCK_ID;
  s_op.lock_mode_ = SHARE;
  queued_normal_op.lock_id_ = DEFAULT_TABLET_LOCK_ID;
  queued_normal_op.lock_mode_ = SHARE;
  queued_normal_op.create_trans_id_ = TRANS_ID2;
  queued_normal_op.owner_id_ = OWNER_ID2;

  // A table-lock continuation must also pass a NORMAL queue head. In the
  // two-phase form, another request can publish its tablet task before this
  // request publishes its own, even though this request has already acquired
  // the table-level lock.
  const ObTableLockPrioArg normal_arg(queued_normal_param.lock_priority_);
  const bool is_two_phase_options[] = {false, true};
  const int64_t two_phase_option_count =
      static_cast<int64_t>(sizeof(is_two_phase_options) / sizeof(is_two_phase_options[0]));
  for (int64_t phase = 0; phase < two_phase_option_count; ++phase) {
    continuation_param.is_two_phase_lock_ = is_two_phase_options[phase];
    ASSERT_EQ(OB_SUCCESS,
              obj_lock_.add_priority_task(queued_normal_param,
                                          store_ctx_normal,
                                          queued_normal_op,
                                          allocator_));
    if (continuation_param.is_two_phase_lock_) {
      ASSERT_EQ(OB_SUCCESS,
                obj_lock_.add_priority_task(continuation_param, store_ctx_s, s_op, allocator_));
    }
    ret = obj_lock_.lock(continuation_param,
                         store_ctx_s,
                         s_op,
                         lock_mode_cnt_in_same_trans,
                         allocator_,
                         conflict_tx_set,
                         &priority_conflict_tx);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, conflict_tx_set.count());
    obj_lock_.remove_lock_op(s_op, allocator_);
    ASSERT_EQ(OB_SUCCESS,
              obj_lock_.remove_priority_task(normal_arg, queued_normal_op, allocator_));
  }
}

} // tablelock
} // transaction
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_obj_lock.log*");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_obj_lock.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_enable_async_log(false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
