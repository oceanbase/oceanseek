/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <cstdint>
#include <gtest/gtest.h>

#define private public
#define protected public
#include "lib/ob_errno.h"
#include "common/rowkey/ob_store_rowkey.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ls/ob_freezer.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_multi_data_source.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"
#include "share/scn.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "share/schema/ob_column_schema.h"
#include "storage/ob_storage_schema.h"
#include "storage/tablet/ob_tablet_multi_source_data.h"
#include "share/config/ob_server_config.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"
#include "storage/memtable/ob_redo_log_generator.h"
#include "lib/container/ob_array_helper.h"
#include "share/ob_fork_table_util.h"
#include "storage/memtable/ob_row_conflict_handler.h"
#include "storage/access/ob_rows_info.h"

namespace oceanbase
{
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace transaction;

namespace concurrent_control
{
int check_sequence_set_violation(const concurrent_control::ObWriteFlag ,
                                 const transaction::ObTxSEQ ,
                                 const transaction::ObTransID ,
                                 const blocksstable::ObDmlFlag ,
                                 const transaction::ObTxSEQ ,
                                 const transaction::ObTransID ,
                                 const blocksstable::ObDmlFlag ,
                                 const transaction::ObTxSEQ )
{
  return OB_SUCCESS;
}
} // concurrent_control

namespace share
{
// This fixture has no tenant ObSharedMemAllocMgr. Bind handles to local
// allocators and bypass tenant throttling, as test_memtable_basic does.
// Keep these overrides in sync with the production allocator interface.
static ObMemstoreAllocator FAKE_DATA_MEMSTORE_ALLOC;
static ObMemstoreAllocator FAKE_MDS_MEMSTORE_ALLOC;
int ObMemstoreAllocator::AllocHandle::bind(const ObMemstoreResourceKind kind)
{
  ObMemstoreAllocator &host = (ObMemstoreResourceKind::MDS == kind)
      ? FAKE_MDS_MEMSTORE_ALLOC
      : FAKE_DATA_MEMSTORE_ALLOC;
  host.register_handle(*this);
  return OB_SUCCESS;
}

// Override the real allocator entry to bypass tenant freezer / throttle
// dependencies that are unavailable in this unit test. The signature must
// match ObMemstoreAllocator::alloc(AllocHandle&, int64_t, const int64_t).
void* ObMemstoreAllocator::alloc(AllocHandle& handle, int64_t size, const int64_t expire_ts)
{
  UNUSED(expire_ts);
  int64_t align_size = upper_align(size, sizeof(int64_t));
  if (!handle.is_id_valid()) {
    COMMON_LOG(TRACE, "MTALLOC.first_alloc", KP(&handle.mt_));
    LockGuard guard(lock_);
    if (!handle.is_id_valid()) {
      handle.set_clock(arena_.retired());
      hlist_.set_active(handle);
    }
  }
  if (arena_.allocator_ == nullptr) {
    if (arena_.init(ObCtxIds::MEMSTORE_CTX_ID, ObNewModIds::OB_MEMSTORE) != OB_SUCCESS) {
      abort();
    }
  }
  return arena_.alloc(handle.id_, handle.arena_handle_, align_size);
}

} // namespace share

namespace storage
{
// override the function make it do nothing
int ObTenantFreezer::unset_tenant_slow_freeze(const common::ObTabletID &tablet_id)
{
  UNUSED(tablet_id);
  return OB_SUCCESS;
}

int ObTxTableGuard::init(ObTxTable *tx_table)
{
  tx_table_ = tx_table;
  epoch_ = 0;
  return OB_SUCCESS;
}

int ObReadInfoStruct::init_compat_version()
{
  compat_version_ = READ_INFO_VERSION_V1;
  return OB_SUCCESS;
}

} // end storage

namespace memtable
{
int ObMvccRow::mvcc_sanity_check_(const share::SCN ,
                                  const concurrent_control::ObWriteFlag ,
                                  ObMvccTransNode &,
                                  ObMvccTransNode *)
{
  return OB_SUCCESS;
}
}

class TestForkReadContext : public testing::Test
{
public:
  TestForkReadContext() : tenant_base_(1), tablet_id_(1000), rowkey_cnt_(1)
  {
    freezer_.init(&ls_);
  }
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, opt_stat_monitor_mgr_.init(1 /* tenant_id */));
    tenant_base_.set(&opt_stat_monitor_mgr_);
    share::ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, mock_col_desc());
    ASSERT_EQ(OB_SUCCESS, mock_iter_param());
  }
  void TearDown() override
  {
    iter_param_.reset();
    read_info_.reset();
    columns_.reset();
    share::ObTenantEnv::set_tenant(nullptr);
    opt_stat_monitor_mgr_.destroy();
  }
  int init_memtable(ObMemtable &mt_table)
  {
    ObITable::TableKey table_key;
    table_key.table_type_ = ObITable::DATA_MEMTABLE;
    table_key.tablet_id_ = ObTabletID(tablet_id_.id());
    table_key.scn_range_.start_scn_ = share::SCN::base_scn();
    table_key.scn_range_.end_scn_ = share::SCN::max_scn();
    int64_t schema_version  = 1;
    uint32_t freeze_clock = 0;
    ObLSHandle ls_handle;
    ls_handle.set_ls(ls_map_, ls_, ObLSGetMod::DATA_MEMTABLE_MOD);

    return mt_table.init(table_key, ls_handle, &freezer_, &memtable_mgr_, schema_version, freeze_clock,
                          true /*use_hash_index*/);
  }
  int mock_col_desc()
  {
    share::schema::ObColDesc col_desc;
    col_desc.col_id_ = OB_APP_MIN_COLUMN_ID;
    col_desc.col_type_.set_type(ObIntType);
    col_desc.col_type_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    columns_.push_back(col_desc);

    share::schema::ObColDesc col_desc2;
    col_desc2.col_id_ = OB_APP_MIN_COLUMN_ID + 1;
    col_desc2.col_type_.set_type(ObIntType);
    col_desc2.col_type_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    columns_.push_back(col_desc2);

    return OB_SUCCESS;
  }
  int mock_iter_param()
  {
    // iter_param_.rowkey_cnt_ = rowkey_cnt_;
    iter_param_.tablet_id_ = tablet_id_;
    iter_param_.table_id_ = tablet_id_.id();
    int ret = read_info_.init(allocator_, 16000, rowkey_cnt_, false, columns_, nullptr/*storage_cols_index*/);
    iter_param_.read_info_ = &read_info_;

    return ret;
  }

  ObLS ls_;
  share::ObTenantBase tenant_base_;
  storage::ObFreezer freezer_;
  storage::ObTabletMemtableMgr memtable_mgr_;
  ObTabletID tablet_id_;
  const int64_t rowkey_cnt_;
  ObTableIterParam iter_param_;
  ObSEArray<share::schema::ObColDesc, 2> columns_;
  ObTableReadInfo read_info_;
  ObArenaAllocator allocator_;
  ObLSMap ls_map_;
  common::ObOptStatMonitorManager opt_stat_monitor_mgr_;
};

class ForkWriteContext
{
public:
  int init(const int64_t trans_id, TestForkReadContext *fixture)
  {
    fixture_ = fixture;
    tx_desc_.tx_id_ = ObTransID(trans_id);
    const share::ObLSID ls_id(1);
    ObAddr scheduler;
    scheduler.parse_from_cstring("127.0.0.1:3001");
    return mem_ctx_.init(MTL_ID(), DATA_CURRENT_VERSION, tx_desc_.tx_id_, ls_id, scheduler);
  }
  // Simulate the redo log submission that a real transaction drives between
  // the writes and the commit: stamp every pending callback with a submitted
  // scn (flips need_submit_log_ off), then end the transaction. Without this,
  // ObTxCallbackList::tx_commit rejects the commit with "callback has not
  // submitted log yet" (see ObTxEndFunctor).
  int end_trans(const bool commit, const share::SCN &trans_version)
  {
    int ret = OB_SUCCESS;
    ObTxCallbackList &list = mem_ctx_.trans_mgr_.callback_list_;
    if (!list.empty()) {
      ObCallbackScope scope;
      scope.start_ = ObITransCallbackIterator(list.get_guard()->get_next());
      scope.end_ = ObITransCallbackIterator(list.get_tail());
      scope.host_ = &list;
      scope.cnt_ = static_cast<int32_t>(list.get_length());
      scope.data_size_ = 0;
      ObArrayHelper<ObCallbackScope> scope_arr(1, &scope, 1);
      int submitted_cnt = 0;
      if (OB_FAIL(mem_ctx_.trans_mgr_.log_submitted(scope_arr, trans_version, submitted_cnt))) {
        TRANS_LOG(WARN, "log submitted failed", K(ret), K(trans_version));
      }
    }
    if (OB_SUCC(ret)) {
      ret = mem_ctx_.do_trans_end(commit, trans_version, trans_version, 0);
    }
    return ret;
  }

  int write(const int64_t key, const int64_t value, ObMemtable &memtable,
            const int64_t snapshot_version = 1000)
  {
    ObStoreCtx store_ctx;
    ObTxSnapshot snapshot;
    concurrent_control::ObWriteFlag write_flag;
    int ret = snapshot.version_.convert_for_gts(snapshot_version);
    if (OB_SUCC(ret)) {
      store_ctx.mvcc_acc_ctx_.init_write_light(mem_ctx_, tx_desc_.tx_id_,
          ObTxSEQ(1000, 0), reinterpret_cast<ObTxTable *>(0x100), snapshot,
          INT64_MAX, INT64_MAX, write_flag);
      ObTableStoreIterator table_iter;
      store_ctx.set_table_iter(&table_iter);
      ObArenaAllocator allocator;
      ObTableAccessContext context;
      ObVersionRange range;
      range.base_version_ = 0;
      range.multi_version_start_ = 0;
      range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
      ObQueryFlag flags;
      flags.use_row_cache_ = ObQueryFlag::DoNotUseCache;
      flags.read_latest_ = ObQueryFlag::OBSF_MASK_READ_LATEST;
      ObDatumRow row;
      if (OB_FAIL(context.init(flags, store_ctx, allocator, range))) {
      } else if (OB_FAIL(row.init(2))) {
      } else {
        row.storage_datums_[0].set_int(key);
        row.storage_datums_[1].set_int(value);
        row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
        const ObMemtableSetArg arg(&row, &fixture_->columns_, nullptr /* update_idx */,
            nullptr /* old_row */, nullptr /* old_row_log_column_idxs */,
            1 /* row_count */, false /* check_exist */,
            nullptr /* encrypt_meta */);
        ret = memtable.set(fixture_->iter_param_, context, arg);
      }
    }
    return ret;
  }
  TestForkReadContext *fixture_ = nullptr;
  ObMemtableCtx mem_ctx_;
  ObTxDesc tx_desc_;
};

TEST_F(TestForkReadContext, fork_source_query_flags)
{
  ObLSHandle ls_handle;
  ls_handle.set_ls(ls_map_, ls_, ObLSGetMod::STORAGE_MOD);
  // Cover each flag alone, every combination, and the ordinary read with no flags.
  for (int mask = 0; mask < 32; ++mask) {
    SCOPED_TRACE(mask);
    ObStoreCtx target_store;
    ObStoreCtx source_store;
    ObTableAccessContext target;
    ObTableAccessContext source;
    ObVersionRange range;
    range.base_version_ = 100;
    range.multi_version_start_ = 200;
    range.snapshot_version_ = 2000;
    ObQueryFlag flags;
    ASSERT_EQ(OB_SUCCESS, target.init(flags, target_store, allocator_, range));

    // Reuse the same source context with different flags, tablet and snapshot.
    // Complementing the mask exercises both set-to-clear and clear-to-set changes.
    for (int reuse = 0; reuse < 2; ++reuse) {
      SCOPED_TRACE(reuse);
      const int current_mask = 0 == reuse ? mask : (mask ^ 31);
      const int64_t fork_snapshot = 1000 + reuse * 100;
      const int64_t target_snapshot = 2000 + reuse * 100;
      const ObTabletID source_tablet(1001 + reuse);
      flags.is_plain_insert_gts_opt_ = 0 != (current_mask & 1);
      flags.read_latest_ = 0 != (current_mask & 2);
      flags.for_foreign_key_check_ = 0 != (current_mask & 4);
      flags.iter_uncommitted_row_ = 0 != (current_mask & 8);
      flags.skip_lock_for_read_ = 0 != (current_mask & 16);
      flags.set_use_fuse_row_cache();
      flags.scan_order_ = 0 == reuse ? ObQueryFlag::Reverse : ObQueryFlag::Forward;
      flags.query_stat_ = 0 == reuse;
      flags.use_block_cache_ = 0 == reuse;
      target.query_flag_ = flags;
      target.tablet_id_ = ObTabletID(1000);
      target.micro_block_handle_mgr_.set_effective_tablet_id(target.tablet_id_);
      range.snapshot_version_ = target_snapshot;
      // Check both preservation below S and clamping above S.
      range.multi_version_start_ = 0 == reuse ? 200 : fork_snapshot + 100;
      target.trans_version_range_ = range;

      share::SCN source_scn;
      share::SCN target_scn;
      ASSERT_EQ(OB_SUCCESS, source_scn.convert_for_tx(fork_snapshot));
      ASSERT_EQ(OB_SUCCESS, target_scn.convert_for_tx(target_snapshot));
      // Use the real read initializer with the fixture's LS handle. LS lookup is
      // outside this unit test; do not fabricate the MVCC snapshot by assignment.
      source_store.reset();
      source_store.tablet_id_ = source_tablet;
      ASSERT_EQ(OB_SUCCESS, source_store.init_for_read(ls_handle, INT64_MAX, 0, source_scn));
      target_store.reset();
      target_store.tablet_id_ = target.tablet_id_;
      ASSERT_EQ(OB_SUCCESS, target_store.init_for_read(ls_handle, INT64_MAX, 5000, target_scn));
      share::ObForkTabletInfo fork_info;
      fork_info.set_src_tablet_id(source_tablet);
      fork_info.set_fork_snapshot_version(fork_snapshot);

      ASSERT_EQ(OB_SUCCESS, source.init_for_fork(target, &source_store, fork_info));
      EXPECT_FALSE(source.query_flag_.is_plain_insert_gts_opt());
      EXPECT_FALSE(source.query_flag_.is_read_latest());
      EXPECT_FALSE(source.query_flag_.is_for_foreign_key_check());
      EXPECT_FALSE(source.query_flag_.iter_uncommitted_row());
      EXPECT_FALSE(source.query_flag_.skip_lock_for_read());
      // The query flag is inherited; the execution switch disables Fork fuse caching.
      EXPECT_TRUE(source.query_flag_.is_use_fuse_row_cache());
      EXPECT_FALSE(source.use_fuse_row_cache_);
      EXPECT_TRUE(target.query_flag_.is_use_fuse_row_cache());
      EXPECT_EQ(flags.flag_, target.query_flag_.flag_);
      EXPECT_EQ(0 == reuse, source.query_flag_.is_reverse_scan());
      EXPECT_EQ(0 == reuse, source.query_flag_.query_stat_);
      EXPECT_EQ(0 == reuse, source.query_flag_.use_block_cache_);
      EXPECT_EQ(fork_snapshot, source.trans_version_range_.snapshot_version_);
      EXPECT_EQ(range.base_version_, source.trans_version_range_.base_version_);
      EXPECT_EQ(0 == reuse ? 200 : fork_snapshot, source.trans_version_range_.multi_version_start_);
      EXPECT_EQ(target_snapshot, target.trans_version_range_.snapshot_version_);
      EXPECT_EQ(range.base_version_, target.trans_version_range_.base_version_);
      EXPECT_EQ(range.multi_version_start_, target.trans_version_range_.multi_version_start_);
      EXPECT_EQ(&source_store, source.store_ctx_);
      EXPECT_EQ(&target_store, target.store_ctx_);
      EXPECT_EQ(source_scn, source.store_ctx_->mvcc_acc_ctx_.get_snapshot_version());
      EXPECT_EQ(target_scn, target.store_ctx_->mvcc_acc_ctx_.get_snapshot_version());
      EXPECT_EQ(source_tablet, source.tablet_id_);
      EXPECT_EQ(source_tablet, source.store_ctx_->tablet_id_);
      EXPECT_EQ(source_tablet, source.micro_block_handle_mgr_.get_effective_tablet_id());
      EXPECT_EQ(ObTabletID(1000), target.tablet_id_);
      EXPECT_EQ(ObTabletID(1000), target.store_ctx_->tablet_id_);
      EXPECT_EQ(ObTabletID(1000), target.micro_block_handle_mgr_.get_effective_tablet_id());
      EXPECT_EQ(0, source.store_ctx_->mvcc_acc_ctx_.tx_lock_timeout_us_);
      EXPECT_EQ(5000, target.store_ctx_->mvcc_acc_ctx_.tx_lock_timeout_us_);
    }
  }
}

TEST_F(TestForkReadContext, fork_source_lock_checks)
{
  ObMemtable dst;
  ObMemtable src;
  ASSERT_EQ(OB_SUCCESS, init_memtable(dst));
  tablet_id_ = ObTabletID(1001);
  ASSERT_EQ(OB_SUCCESS, init_memtable(src));
  tablet_id_ = ObTabletID(1000);

  ForkWriteContext owner;
  ForkWriteContext other;
  ASSERT_EQ(OB_SUCCESS, owner.init(21, this));
  ASSERT_EQ(OB_SUCCESS, other.init(22, this));
  // 四个键分别只在源或目标上持锁，明确区分“锁属于谁”和“锁属于哪张表”。
  ASSERT_EQ(OB_SUCCESS, owner.write(1, 10, src));
  ASSERT_EQ(OB_SUCCESS, other.write(2, 20, src));
  ASSERT_EQ(OB_SUCCESS, owner.write(3, 30, dst));
  ASSERT_EQ(OB_SUCCESS, other.write(4, 40, dst));

  // 源的最新提交版本超过 Fork SCN，检查时必须回到旧的已提交版本。
  share::SCN old_scn;
  share::SCN new_scn;
  ASSERT_EQ(OB_SUCCESS, old_scn.convert_for_tx(900));
  ASSERT_EQ(OB_SUCCESS, new_scn.convert_for_tx(1100));
  {
    ForkWriteContext committed;
    ASSERT_EQ(OB_SUCCESS, committed.init(23, this));
    ASSERT_EQ(OB_SUCCESS, committed.write(5, 50, src));
    ASSERT_EQ(OB_SUCCESS, committed.end_trans(true, old_scn));
  }
  {
    ForkWriteContext committed;
    ASSERT_EQ(OB_SUCCESS, committed.init(24, this));
    ASSERT_EQ(OB_SUCCESS, committed.write(5, 500, src, 1200));
    ASSERT_EQ(OB_SUCCESS, committed.end_trans(true, new_scn));
  }

  share::ObForkTabletInfo fork_info;
  fork_info.set_src_tablet_id(ObTabletID(1001));
  fork_info.set_fork_snapshot_version(1000);
  ObTableStoreIterator table_iter;
  ObStoreCtx store_ctx;
  ObTxSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, snapshot.version_.convert_for_tx(2000));
  concurrent_control::ObWriteFlag write_flag;
  store_ctx.mvcc_acc_ctx_.init_write_light(owner.mem_ctx_, owner.tx_desc_.tx_id_,
      ObTxSEQ(1000, 0), reinterpret_cast<ObTxTable *>(0x100),
      snapshot, INT64_MAX, INT64_MAX, write_flag);
  store_ctx.tablet_id_ = ObTabletID(1000);
  ObVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 0;
  version_range.snapshot_version_ = 2000;
  ObTableAccessContext context;
  ObQueryFlag query_flag;
  ASSERT_EQ(OB_SUCCESS, context.init(query_flag, store_ctx, allocator_, version_range));

  // 直接调用真实共享入口：不替换锁判断，也不在测试中提前设置源检查模式。
  // prelock 的 SQL 生产者仅为 Oracle DBMS_LOB，而 Fork 不支持 Oracle，故在存储层验证。
  for (int order = 0; order < 2; ++order) {
    table_iter.reset();
    ASSERT_EQ(OB_SUCCESS, table_iter.add_table(order == 0 ? &src : &dst));
    ASSERT_EQ(OB_SUCCESS, table_iter.add_table(order == 0 ? &dst : &src));
    table_iter.set_fork_info(&fork_info);
    store_ctx.set_table_iter(&table_iter);
    for (int64_t key = 1; key <= 5; ++key) {
      ObStorageDatum datum;
      datum.set_int(key);
      ObDatumRowkey rowkey;
      rowkey.assign(&datum, 1);
      ASSERT_EQ(OB_SUCCESS, rowkey.prepare_memtable_readable(columns_, allocator_));
      for (int by_myself = 0; by_myself < 2; ++by_myself) {
        SCOPED_TRACE(::testing::Message() << "order=" << order
            << ", key=" << key << ", by_myself=" << by_myself);
        const int expected = ((by_myself && key == 3) || (!by_myself && key == 4))
            ? OB_TRY_LOCK_ROW_CONFLICT : OB_SUCCESS;
        EXPECT_EQ(expected, ObRowConflictHandler::check_row_locked(
            iter_param_, context, rowkey, by_myself != 0, false /* post_lock */));
        // 含提前 break 的目标锁路径也必须恢复，下一轮不能继承源检查模式。
        EXPECT_EQ(snapshot.version_, store_ctx.mvcc_acc_ctx_.get_snapshot_version());
        EXPECT_EQ(owner.tx_desc_.tx_id_, store_ctx.mvcc_acc_ctx_.get_tx_id());
        EXPECT_FALSE(store_ctx.mvcc_acc_ctx_.is_fork_source_check_);
      }
    }
  }

  // 批量入口用两个真实源行验证，避免把“多行 SQL”误当作批量存储分支覆盖。
  // 仅组装 RowsInfo 的输入元数据，下面调用生产的逐 store 包装及 MVCC 检查。
  ObDatumRow rows[2];
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(2));
    rows[i].storage_datums_[0].set_int(i == 0 ? 2 : 5);
    rows[i].storage_datums_[1].set_int(0);
    rows[i].row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
  }
  ObRowsInfo rows_info;
  rows_info.is_inited_ = true;
  rows_info.exist_helper_.is_inited_ = true;
  rows_info.col_descs_ = &columns_;
  rows_info.rowkey_column_num_ = 1;
  ObStoreRowLockState states[2];
  auto prepare_rows = [&]() {
    const int ret = rows_info.assign_rows(2, rows);
    for (int64_t i = 0; ret == OB_SUCCESS && i < 2; ++i) {
      states[i].reset();
      rows_info.set_row_lock_state(i, &states[i]);
    }
    return ret;
  };
  share::SCN max_version;
  ASSERT_EQ(OB_SUCCESS, prepare_rows());
  EXPECT_EQ(OB_SUCCESS, dst.check_rows_locked_on_frozen_store_(
      iter_param_, context, false, &src, rows_info, max_version));
  EXPECT_FALSE(rows_info.have_conflict());
  EXPECT_FALSE(states[0].is_locked_);
  EXPECT_FALSE(states[1].is_locked_);
  EXPECT_EQ(old_scn, states[1].trans_version_);
  EXPECT_TRUE(states[1].row_exist());
  EXPECT_EQ(snapshot.version_, store_ctx.mvcc_acc_ctx_.get_snapshot_version());
  EXPECT_FALSE(store_ctx.mvcc_acc_ctx_.is_fork_source_check_);

  // 同一包装切回目标存储后，其他事务的真实锁仍必须报告冲突。
  rows[0].storage_datums_[0].set_int(4);
  ASSERT_EQ(OB_SUCCESS, prepare_rows());
  EXPECT_EQ(OB_TRY_LOCK_ROW_CONFLICT, dst.check_rows_locked_on_frozen_store_(
      iter_param_, context, false, &dst, rows_info, max_version));
  EXPECT_TRUE(rows_info.have_conflict());
  EXPECT_EQ(other.tx_desc_.tx_id_, states[0].lock_trans_id_);
  EXPECT_EQ(snapshot.version_, store_ctx.mvcc_acc_ctx_.get_snapshot_version());
  EXPECT_FALSE(store_ctx.mvcc_acc_ctx_.is_fork_source_check_);

  ASSERT_EQ(OB_SUCCESS, owner.end_trans(false, snapshot.version_));
  ASSERT_EQ(OB_SUCCESS, other.end_trans(false, snapshot.version_));
}


} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_fork_read_context.log", true);
  OB_LOGGER.set_log_level("INFO");
  // This fixture has no LockWaitMgr; disable deadlock detector registration.
  GCONF._lcl_op_interval = 0;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
