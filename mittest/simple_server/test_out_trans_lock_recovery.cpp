// owner: yangyifei.yyf
// owner group: transaction

/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <string>

#include <gtest/gtest.h>

#define USING_LOG_PREFIX TABLELOCK
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "env/ob_simple_server_restart_helper.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/checkpoint/ob_checkpoint_executor.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tablelock/ob_lock_table.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_log.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_table/ob_tx_ctx_memtable.h"
#include "storage/tx_table/ob_tx_table.h"

#undef private
#undef protected

static const char *TEST_FILE_NAME = "test_out_trans_lock_recovery";
static const char *BORN_CASE_NAME = "ObOutTransLockBeforeRestartTest";
static const char *RESTART_CASE_NAME = "ObOutTransLockAfterRestartTest";

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::storage;
using namespace oceanbase::transaction;
using namespace oceanbase::transaction::tablelock;
using namespace oceanbase::share;
using namespace oceanbase::storage::checkpoint;

class ObOutTransLockBeforeRestartTest : public ObSimpleClusterTestBase
{
public:
  ObOutTransLockBeforeRestartTest()
    : ObSimpleClusterTestBase(TEST_FILE_NAME)
  {}

protected:
  struct ExpectedTxCtxSnapshot
  {
    ExpectedTxCtxSnapshot()
      : tx_id_(),
        owner_id_(),
        op_type_(UNKNOWN_TYPE),
        tx_ctx_count_(0),
        lock_op_count_(0),
        is_init_state_(false),
        has_valid_durable_scn_(false)
    {}

    ExpectedTxCtxSnapshot(const ObTransID &tx_id,
                          const ObTableLockOwnerID &owner_id,
                          const ObTableLockOpType op_type)
      : tx_id_(tx_id),
        owner_id_(owner_id),
        op_type_(op_type),
        tx_ctx_count_(0),
        lock_op_count_(0),
        is_init_state_(false),
        has_valid_durable_scn_(false)
    {}

    TO_STRING_KV(K_(tx_id),
                 K_(owner_id),
                 K_(op_type),
                 K_(tx_ctx_count),
                 K_(lock_op_count),
                 K_(is_init_state),
                 K_(has_valid_durable_scn));

    ObTransID tx_id_;
    ObTableLockOwnerID owner_id_;
    ObTableLockOpType op_type_;
    int64_t tx_ctx_count_;
    int64_t lock_op_count_;
    bool is_init_state_;
    bool has_valid_durable_scn_;
  };

  class TxCtxSnapshotCollector : public ObTxCtxTableRecoverHelper
  {
  public:
    explicit TxCtxSnapshotCollector(
        common::ObIArray<ExpectedTxCtxSnapshot> &expected_snapshots)
      : expected_snapshots_(expected_snapshots)
    {}

  private:
    int recover_one_tx_ctx_(ObLSTxCtxMgr *ls_tx_ctx_mgr,
                            ObTxCtxTableInfo &ctx_info) override
    {
      UNUSED(ls_tx_ctx_mgr);
      for (int64_t i = 0; i < expected_snapshots_.count(); ++i) {
        ExpectedTxCtxSnapshot &expected = expected_snapshots_.at(i);
        if (expected.tx_id_ == ctx_info.tx_id_) {
          ++expected.tx_ctx_count_;
          expected.is_init_state_ = ObTxState::INIT == ctx_info.exec_info_.state_;
          expected.has_valid_durable_scn_ =
              ctx_info.table_lock_info_.max_durable_scn_.is_valid();
          for (int64_t j = 0;
               j < ctx_info.table_lock_info_.table_lock_ops_.count();
               ++j) {
            const transaction::tablelock::ObTableLockOp &lock_op =
                ctx_info.table_lock_info_.table_lock_ops_.at(j);
            if (expected.owner_id_ == lock_op.owner_id_
                && expected.op_type_ == lock_op.op_type_
                && LOCK_OP_DOING == lock_op.lock_op_status_) {
              ++expected.lock_op_count_;
            }
          }
        }
      }
      return OB_SUCCESS;
    }

    common::ObIArray<ExpectedTxCtxSnapshot> &expected_snapshots_;
  };

  void get_table_id_(const uint64_t tenant_id,
                     const char *table_name,
                     uint64_t &table_id)
  {
    ObSchemaGetterGuard schema_guard;
    ASSERT_EQ(OB_SUCCESS,
              GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
    ASSERT_EQ(OB_SUCCESS,
              schema_guard.get_table_id(tenant_id,
                                        "test",
                                        table_name,
                                        false,
                                        share::schema::ObSchemaGetterGuard::NON_TEMP_WITH_NON_HIDDEN_TABLE_TYPE,
                                        table_id));
  }

  void get_lock_service_ls_(ObLS *&ls, ObLSHandle &ls_handle)
  {
    ObLSService *ls_service = MTL(ObLSService *);
    ASSERT_NE(nullptr, ls_service);
    ASSERT_EQ(OB_SUCCESS,
              ls_service->get_ls(share::LOCK_SERVICE_LS,
                                 ls_handle,
                                 ObLSGetMod::TABLELOCK_MOD));
    ASSERT_NE(nullptr, ls = ls_handle.get_ls());
  }

  void get_lock_memtable_(ObLS *ls,
                          ObTableHandleV2 &table_handle,
                          ObLockMemtable *&lock_memtable)
  {
    ASSERT_NE(nullptr, ls);
    ASSERT_EQ(OB_SUCCESS, ls->lock_table_.get_lock_memtable(table_handle));
    ASSERT_EQ(OB_SUCCESS, table_handle.get_lock_memtable(lock_memtable));
    ASSERT_NE(nullptr, lock_memtable);
  }

  void get_tx_ctx_memtable_(ObLS *ls, ObTxCtxMemtable *&tx_ctx_memtable)
  {
    tx_ctx_memtable = nullptr;
    ASSERT_NE(nullptr, ls);
    ObCheckpointExecutor *checkpoint_executor = ls->get_checkpoint_executor();
    ASSERT_NE(nullptr, checkpoint_executor);
    ObLSTxService *tx_service = dynamic_cast<ObLSTxService *>(
        checkpoint_executor->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE]);
    ASSERT_NE(nullptr, tx_service);
    tx_ctx_memtable = dynamic_cast<ObTxCtxMemtable *>(
        tx_service->common_checkpoints_[ObCommonCheckpointType::TX_CTX_MEMTABLE_TYPE]);
    ASSERT_NE(nullptr, tx_ctx_memtable);
  }

  void execute_sys_sql_(const char *sql)
  {
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, exec_write_sql_sys(sql, affected_rows));
  }

  void configure_tx_ctx_capture_(const char *signal, const int64_t expected_capture_count)
  {
    const int64_t debug_sync_timeout_us = 60L * 1000 * 1000;
    ObSqlString sql;
    ASSERT_GT(expected_capture_count, 0);
    execute_sys_sql_("alter system set debug_sync_timeout = '60s'");
    for (int64_t i = 0;
         i < 100 && debug_sync_timeout_us != static_cast<int64_t>(GCONF.debug_sync_timeout);
         ++i) {
      usleep(100 * 1000);
    }
    ASSERT_EQ(debug_sync_timeout_us,
              static_cast<int64_t>(GCONF.debug_sync_timeout));
    execute_sys_sql_("set ob_global_debug_sync = 'reset'");
    ASSERT_EQ(OB_SUCCESS,
              sql.assign_fmt("set ob_global_debug_sync = "
                             "'AFTER_TX_CTX_TABLE_LOCK_INFO_CAPTURE signal %s execute %ld'",
                             signal,
                             expected_capture_count));
    execute_sys_sql_(sql.ptr());
  }

  void wait_for_tx_ctx_capture_(const char *signal, const int64_t expected_capture_count)
  {
    ObSqlString sql;
    ASSERT_GT(expected_capture_count, 0);
    for (int64_t i = 0; i < expected_capture_count; ++i) {
      ASSERT_EQ(OB_SUCCESS,
                sql.assign_fmt("set ob_global_debug_sync = 'now wait_for %s execute 1'", signal));
      const int64_t wait_start_us = ObTimeUtility::current_monotonic_time();
      execute_sys_sql_(sql.ptr());
      const int64_t wait_elapsed_us =
          ObTimeUtility::current_monotonic_time() - wait_start_us;
      // ObDSEventControl::wait() returns success even after its 60-second
      // timeout. A valid signal is already queued after the flush completes,
      // so a long SQL call means the synchronization point was not reached.
      ASSERT_LT(wait_elapsed_us, 30L * 1000 * 1000);
    }
  }

  void clear_tx_ctx_capture_()
  {
    execute_sys_sql_("set ob_global_debug_sync = 'AFTER_TX_CTX_TABLE_LOCK_INFO_CAPTURE clear'");
  }

  int flush_tx_ctx_and_wait_(ObLS *ls)
  {
    int ret = OB_SUCCESS;
    ObTxCtxMemtable *tx_ctx_memtable = nullptr;
    get_tx_ctx_memtable_(ls, tx_ctx_memtable);
    if (OB_ISNULL(tx_ctx_memtable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tx ctx memtable is null", K(ret));
    } else {
      for (int64_t i = 0; i < 20; ++i) {
        ret = ls->ls_tx_svr_.flush_ls_inner_tablet(LS_TX_CTX_TABLET);
        if (OB_SUCCESS == ret) {
          break;
        } else if (OB_EAGAIN != ret) {
          LOG_WARN("flush tx ctx memtable failed", K(ret), K(i));
          break;
        }
        usleep(100 * 1000);
      }
      for (int64_t i = 0;
           OB_SUCC(ret) && i < 300 && tx_ctx_memtable->is_frozen_memtable();
           ++i) {
        usleep(100 * 1000);
      }
      if (OB_SUCC(ret) && tx_ctx_memtable->is_frozen_memtable()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait tx ctx memtable flush timeout", K(ret));
      }
    }
    return ret;
  }

  int scan_persisted_tx_ctx_snapshots_(ObLS *ls,
                                       TxCtxSnapshotCollector &collector)
  {
    int ret = OB_SUCCESS;
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    const ObSSTableArray *sstables = nullptr;
    ObStorageMetaHandle sstable_handle;
    blocksstable::ObSSTable *sstable = nullptr;
    ObStoreRowIterator *row_iter = nullptr;
    const blocksstable::ObDatumRow *row = nullptr;
    common::ObArenaAllocator allocator;
    blocksstable::ObDatumRange whole_range;
    ObStoreCtx store_ctx;
    ObTableAccessContext access_context;
    common::ObQueryFlag query_flag;
    common::ObVersionRange trans_version_range;
    common::ObSEArray<share::schema::ObColDesc, 3> columns;
    ObTableReadInfo read_info;
    ObTableIterParam iter_param;
    ObLSTabletService *tablet_service = nullptr;
    ObTxTable *tx_table = nullptr;
    ObLSTxService *tx_service = nullptr;

    whole_range.set_whole_range();
    query_flag.use_row_cache_ = common::ObQueryFlag::DoNotUseCache;
    trans_version_range.base_version_ = 0;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.snapshot_version_ = MERGE_READ_SNAPSHOT_VERSION;

    share::schema::ObColDesc key;
    key.col_id_ = OB_APP_MIN_COLUMN_ID;
    key.col_type_.set_int();
    key.col_order_ = common::ObOrderType::ASC;
    share::schema::ObColDesc meta;
    meta.col_id_ = OB_APP_MIN_COLUMN_ID + 1;
    meta.col_type_.set_binary();
    meta.col_order_ = common::ObOrderType::ASC;
    share::schema::ObColDesc value;
    value.col_id_ = OB_APP_MIN_COLUMN_ID + 2;
    value.col_type_.set_binary();
    value.col_order_ = common::ObOrderType::ASC;

    iter_param.tablet_id_ = LS_TX_CTX_TABLET;
    iter_param.table_id_ = 1;
    if (OB_ISNULL(ls)
        || OB_ISNULL(tablet_service = ls->get_tablet_svr())
        || OB_ISNULL(tx_table = ls->get_tx_table())
        || OB_ISNULL(tx_service = ls->get_tx_svr())
        || OB_ISNULL(tx_service->mgr_)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(tablet_service->get_tablet(LS_TX_CTX_TABLET, tablet_handle))) {
      LOG_WARN("get tx ctx tablet failed", K(ret));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tx ctx tablet is null", K(ret));
    } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
      LOG_WARN("fetch tx ctx table store failed", K(ret));
    } else if (FALSE_IT(sstables = &table_store_wrapper.get_member()->get_minor_sstables())) {
    } else if (OB_UNLIKELY(sstables->empty())) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("tx ctx sstable is empty", K(ret));
    } else if (OB_ISNULL(sstable = sstables->at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tx ctx sstable is null", K(ret));
    } else if (!sstable->is_loaded()
               && OB_FAIL(ObCacheSSTableHelper::load_sstable(
                   sstable->get_addr(), sstable->get_meta_type(), sstable_handle))) {
      LOG_WARN("load tx ctx sstable failed", K(ret), KPC(sstable));
    } else if (!sstable->is_loaded()
               && OB_FAIL(sstable_handle.get_sstable(sstable))) {
      LOG_WARN("get loaded tx ctx sstable failed", K(ret));
    } else if (OB_FAIL(access_context.init(
                   query_flag, store_ctx, allocator, trans_version_range))) {
      LOG_WARN("init tx ctx sstable access context failed", K(ret));
    } else if (OB_FAIL(columns.push_back(key))
               || OB_FAIL(columns.push_back(meta))
               || OB_FAIL(columns.push_back(value))) {
      LOG_WARN("build tx ctx sstable read columns failed", K(ret));
    } else if (OB_FAIL(read_info.init(allocator,
                                      ObTxTable::LS_TX_CTX_SCHEMA_COLUMN_CNT,
                                      ObTxTable::LS_TX_CTX_SCHEMA_ROWKEY_CNT,
                                      lib::is_oracle_mode(),
                                      columns,
                                      nullptr /*storage_cols_index*/))) {
      LOG_WARN("init tx ctx sstable read info failed", K(ret));
    } else if (FALSE_IT(iter_param.read_info_ = &read_info)) {
    } else if (OB_FAIL(sstable->scan(iter_param,
                                     access_context,
                                     whole_range,
                                     row_iter))) {
      LOG_WARN("scan tx ctx sstable failed", K(ret));
    } else if (OB_ISNULL(row_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tx ctx sstable iterator is null", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(row_iter->get_next_row(row))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get tx ctx sstable row failed", K(ret));
          }
          break;
        } else if (OB_FAIL(collector.recover(
                       *row, *tx_table->get_tx_data_table(), tx_service->mgr_))) {
          LOG_WARN("collect persisted tx ctx snapshot failed", K(ret));
        }
      }
    }

    if (OB_NOT_NULL(row_iter)) {
      row_iter->~ObStoreRowIterator();
      row_iter = nullptr;
    }
    return ret;
  }

  void verify_persisted_tx_ctx_snapshots_(
      ObLS *ls,
      common::ObIArray<ExpectedTxCtxSnapshot> &expected_snapshots)
  {
    TxCtxSnapshotCollector collector(expected_snapshots);
    ASSERT_EQ(OB_SUCCESS, scan_persisted_tx_ctx_snapshots_(ls, collector));
    for (int64_t i = 0; i < expected_snapshots.count(); ++i) {
      const ExpectedTxCtxSnapshot &expected = expected_snapshots.at(i);
      EXPECT_EQ(1, expected.tx_ctx_count_) << "tx_id=" << expected.tx_id_.get_id();
      EXPECT_EQ(1, expected.lock_op_count_) << "tx_id=" << expected.tx_id_.get_id();
      EXPECT_TRUE(expected.is_init_state_) << "tx_id=" << expected.tx_id_.get_id();
      EXPECT_TRUE(expected.has_valid_durable_scn_)
          << "tx_id=" << expected.tx_id_.get_id();
    }
  }

  void wait_for_new_lock_commit_(ObLockMemtable *lock_memtable,
                                 const SCN &previous_commit_scn,
                                 SCN &commit_scn)
  {
    for (int64_t i = 0; i < 300; ++i) {
      commit_scn = lock_memtable->max_committed_scn_.atomic_load();
      if (commit_scn > previous_commit_scn) {
        break;
      }
      usleep(100 * 1000);
    }
    ASSERT_GT(commit_scn, previous_commit_scn);
  }

  int submit_out_trans_mds_redo_(ObLS *ls, const ObTransID &tx_id, SCN &mds_scn)
  {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    ObPartTransCtx *tx_ctx = nullptr;
    bool has_table_lock_mds = false;
    mds_scn = SCN::min_scn();

    if (OB_ISNULL(ls)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ls is null", K(ret), K(tx_id));
    } else if (OB_FAIL(ls->get_tx_ctx(tx_id, false, tx_ctx))) {
      LOG_WARN("get tx ctx failed", K(ret), K(tx_id));
    } else if (OB_ISNULL(tx_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tx ctx is null", K(ret), K(tx_id));
    } else {
      for (int64_t i = 0; i < 20; ++i) {
        {
          CtxLockGuard guard(tx_ctx->lock_);
          ret = tx_ctx->submit_log_impl_(ObTxLogType::TX_MULTI_DATA_SOURCE_LOG);
        }
        if (OB_SUCCESS == ret) {
          break;
        } else if (OB_EAGAIN != ret && OB_TX_NOLOGCB != ret) {
          LOG_WARN("submit table lock MDS redo failed", K(ret), K(i), K(tx_id));
          break;
        }
        usleep(100 * 1000);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < 300 && !has_table_lock_mds; ++i) {
        {
          CtxLockGuard guard(tx_ctx->lock_);
          mds_scn = tx_ctx->exec_info_.max_applying_log_ts_;
          for (int64_t j = 0; j < tx_ctx->exec_info_.multi_data_source_.count(); ++j) {
            const ObTxBufferNode &mds_node =
                tx_ctx->exec_info_.multi_data_source_.at(j);
            if (ObTxDataSourceType::TABLE_LOCK == mds_node.get_data_source_type()
                && mds_node.is_synced()) {
              has_table_lock_mds = true;
              break;
            }
          }
        }
        if (!has_table_lock_mds) {
          usleep(100 * 1000);
        }
      }
      if (OB_SUCC(ret) && !has_table_lock_mds) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait durable table lock MDS timeout", K(ret), K(tx_id));
      } else if (OB_SUCC(ret) && !mds_scn.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table lock MDS scn", K(ret), K(tx_id), K(mds_scn));
      }
    }
    if (OB_NOT_NULL(ls) && OB_NOT_NULL(tx_ctx)
        && OB_TMP_FAIL(ls->revert_tx_ctx(tx_ctx))) {
      LOG_WARN("revert tx ctx failed", K(tmp_ret), K(tx_id));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
    return ret;
  }

  void prepare_out_trans_tx_(ObLS *ls,
                             const uint64_t table_id,
                             const ObTableLockOwnerID &owner_id,
                             const bool is_unlock,
                             ObTxDesc *&tx_desc)
  {
    ObTransService *tx_service = MTL(ObTransService *);
    ObTableLockService *table_lock_service = MTL(ObTableLockService *);
    ObTxParam tx_param;
    tx_desc = nullptr;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.timeout_us_ = 300 * 1000 * 1000L;
    tx_param.lock_timeout_us_ = -1;
    tx_param.cluster_id_ = GCONF.cluster_id;

    ASSERT_NE(nullptr, tx_service);
    ASSERT_NE(nullptr, table_lock_service);
    ASSERT_EQ(OB_SUCCESS, tx_service->acquire_tx(tx_desc));
    ASSERT_NE(nullptr, tx_desc);

    if (is_unlock) {
      ObUnLockTableRequest request;
      if (!request.is_unlock_request()) {
        request.set_to_unlock_type();
      }
      request.table_id_ = table_id;
      request.owner_id_ = owner_id;
      request.lock_mode_ = EXCLUSIVE;
      request.op_type_ = OUT_TRANS_UNLOCK;
      request.timeout_us_ = 0;
      ASSERT_EQ(OB_SUCCESS, table_lock_service->unlock(*tx_desc, tx_param, request));
    } else {
      ObLockTableRequest request;
      request.table_id_ = table_id;
      request.owner_id_ = owner_id;
      request.lock_mode_ = EXCLUSIVE;
      request.op_type_ = OUT_TRANS_LOCK;
      request.timeout_us_ = 0;
      ASSERT_EQ(OB_SUCCESS, table_lock_service->lock(*tx_desc, tx_param, request));
    }

    // Small TABLE_LOCK MDS records normally stay in the in-memory MDS cache
    // until transaction end. Submit its regular redo first so this active tx
    // context can be checkpointed with a real pre-commit DOING snapshot.
    SCN mds_scn = SCN::min_scn();
    ASSERT_EQ(OB_SUCCESS, submit_out_trans_mds_redo_(ls, tx_desc->tid(), mds_scn));
    ASSERT_GT(mds_scn, SCN::min_scn());
  }

  int submit_commit_info_and_wait_(ObLS *ls,
                                   const ObTransID &tx_id,
                                   SCN &commit_info_scn)
  {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    ObPartTransCtx *tx_ctx = nullptr;
    bool redo_complete = false;
    commit_info_scn = SCN::min_scn();

    if (OB_ISNULL(ls)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ls is null", K(ret), K(tx_id));
    } else if (OB_FAIL(ls->get_tx_ctx(tx_id, false, tx_ctx))) {
      LOG_WARN("get tx ctx failed", K(ret), K(tx_id));
    } else if (OB_ISNULL(tx_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tx ctx is null", K(ret), K(tx_id));
    } else {
      for (int64_t i = 0; i < 20; ++i) {
        {
          CtxLockGuard guard(tx_ctx->lock_);
          ret = tx_ctx->submit_log_impl_(ObTxLogType::TX_COMMIT_INFO_LOG);
        }
        if (OB_SUCCESS == ret) {
          break;
        } else if (OB_EAGAIN != ret && OB_TX_NOLOGCB != ret) {
          LOG_WARN("submit commit info failed", K(ret), K(i), K(tx_id));
          break;
        }
        usleep(100 * 1000);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < 300 && !redo_complete; ++i) {
        {
          CtxLockGuard guard(tx_ctx->lock_);
          redo_complete = ObTxState::REDO_COMPLETE == tx_ctx->exec_info_.state_;
          commit_info_scn = tx_ctx->exec_info_.max_applied_log_ts_;
          if (redo_complete) {
            // Keep the old leader from advancing this synthetic transaction to
            // its final commit before the restart is triggered.
            ret = tx_ctx->unregister_timeout_task_();
          }
        }
        if (!redo_complete) {
          usleep(100 * 1000);
        }
      }
      if (OB_SUCC(ret) && !redo_complete) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait redo complete timeout", K(ret), K(tx_id));
      } else if (OB_SUCC(ret) && !(commit_info_scn > SCN::min_scn())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid commit info scn", K(ret), K(tx_id), K(commit_info_scn));
      }
    }
    if (OB_NOT_NULL(ls) && OB_NOT_NULL(tx_ctx)
        && OB_TMP_FAIL(ls->revert_tx_ctx(tx_ctx))) {
      LOG_WARN("revert tx ctx failed", K(tmp_ret), K(tx_id));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
    return ret;
  }

  void capture_out_trans_tx_ctx_snapshots_(ObLS *ls,
                                            const char *capture_signal,
                                            common::ObIArray<ExpectedTxCtxSnapshot> &expected_snapshots)
  {
    const int64_t expected_capture_count = expected_snapshots.count();
    configure_tx_ctx_capture_(capture_signal, expected_capture_count);
    ASSERT_EQ(OB_SUCCESS, flush_tx_ctx_and_wait_(ls));
    wait_for_tx_ctx_capture_(capture_signal, expected_capture_count);
    clear_tx_ctx_capture_();
    verify_persisted_tx_ctx_snapshots_(ls, expected_snapshots);
  }

  void commit_out_trans_tx_(ObTxDesc *&tx_desc)
  {
    ObTransService *tx_service = MTL(ObTransService *);
    ASSERT_NE(nullptr, tx_service);
    ASSERT_NE(nullptr, tx_desc);
    ASSERT_EQ(OB_SUCCESS,
              tx_service->commit_tx(*tx_desc, ObTimeUtility::current_time() + 60 * 1000 * 1000L));
    ASSERT_EQ(OB_SUCCESS, tx_service->release_tx(*tx_desc));
    tx_desc = nullptr;
  }

  int flush_lock_table_(ObLockMemtable *lock_memtable, const SCN &target_scn)
  {
    int ret = OB_SUCCESS;
    bool flush_finished = false;
    if (OB_ISNULL(lock_memtable)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("lock memtable is null", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < 300; ++i) {
        const int flush_ret =
            lock_memtable->flush(SCN::max_scn(), checkpoint::INVALID_TRACE_ID, true);
        if (OB_SUCCESS != flush_ret
            && OB_EAGAIN != flush_ret
            && OB_SIZE_OVERFLOW != flush_ret) {
          ret = flush_ret;
          LOG_WARN("flush lock memtable failed", K(ret), K(i), K(target_scn));
        } else if (!lock_memtable->is_frozen_memtable()
                   && lock_memtable->flushed_scn_ >= target_scn) {
          flush_finished = true;
          break;
        } else {
          usleep(100 * 1000);
        }
      }
      if (OB_SUCC(ret) && !flush_finished) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait lock memtable flush timeout",
                 K(ret), K(target_scn), K(lock_memtable->flushed_scn_));
      }
    }
    return ret;
  }

  int has_lock_op_status_(ObLockMemtable *lock_memtable,
                          const uint64_t table_id,
                          const ObTableLockOwnerID &owner_id,
                          const ObTableLockOpType op_type,
                          const ObTableLockOpStatus status,
                          bool &found)
  {
    int ret = OB_SUCCESS;
    ObLockID lock_id;
    ObLockOpIterator op_iter;
    found = false;
    if (OB_ISNULL(lock_memtable)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("lock memtable is null", K(ret), K(table_id));
    } else if (OB_FAIL(get_lock_id(table_id, lock_id))) {
      LOG_WARN("get lock id failed", K(ret), K(table_id));
    } else if (OB_FAIL(lock_memtable->get_lock_op_iter(lock_id, op_iter))) {
      LOG_WARN("get lock op iterator failed", K(ret), K(lock_id));
    } else {
      transaction::tablelock::ObTableLockOp lock_op;
      while (!found && OB_SUCC(ret)) {
        if (OB_FAIL(op_iter.get_next(lock_op))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("iterate lock op failed", K(ret), K(lock_id));
          }
        } else {
          found = owner_id == lock_op.owner_id_
               && op_type == lock_op.op_type_
               && status == lock_op.lock_op_status_;
        }
      }
    }
    return ret;
  }

};

TEST_F(ObOutTransLockBeforeRestartTest, make_checkpoint_skew)
{
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS,
            sql.assign("create table out_trans_lock_recovery_t "
                       "(c1 int primary key)"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS,
            sql.assign("create table out_trans_lock_snapshot_t "
                       "(c1 int primary key)"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS,
            sql.assign("create table out_trans_lock_unresolved_t "
                       "(c1 int primary key)"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  uint64_t unlock_table_id = 0;
  uint64_t stale_lock_table_id = 0;
  uint64_t unresolved_lock_table_id = 0;
  get_table_id_(tenant_id, "out_trans_lock_recovery_t", unlock_table_id);
  get_table_id_(tenant_id, "out_trans_lock_snapshot_t", stale_lock_table_id);
  get_table_id_(tenant_id, "out_trans_lock_unresolved_t", unresolved_lock_table_id);

  ObLS *lock_ls = nullptr;
  ObLSHandle ls_handle;
  get_lock_service_ls_(lock_ls, ls_handle);
  ObTableHandleV2 table_handle;
  ObLockMemtable *lock_memtable = nullptr;
  get_lock_memtable_(lock_ls, table_handle, lock_memtable);

  ObTableLockOwnerID unlock_owner;
  ObTableLockOwnerID final_lock_owner;
  ObTableLockOwnerID stale_lock_owner;
  ObTableLockOwnerID unresolved_lock_owner;
  ASSERT_EQ(OB_SUCCESS,
            unlock_owner.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE, 1001));
  ASSERT_EQ(OB_SUCCESS,
            final_lock_owner.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE, 1002));
  ASSERT_EQ(OB_SUCCESS,
            stale_lock_owner.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE, 1003));
  ASSERT_EQ(OB_SUCCESS,
            unresolved_lock_owner.convert_from_value(
                ObLockOwnerType::DEFAULT_OWNER_TYPE, 1004));

  ObTableLockService *table_lock_service = MTL(ObTableLockService *);
  ASSERT_NE(nullptr, table_lock_service);
  ASSERT_EQ(OB_SUCCESS, table_lock_service->lock_table(unlock_table_id, EXCLUSIVE, unlock_owner));

  SCN initial_lock_commit_scn = SCN::min_scn();
  wait_for_new_lock_commit_(lock_memtable, SCN::min_scn(), initial_lock_commit_scn);

  ObTxDesc *unlock_tx_desc = nullptr;
  ObTxDesc *stale_lock_tx_desc = nullptr;
  ObTxDesc *unresolved_lock_tx_desc = nullptr;
  prepare_out_trans_tx_(lock_ls,
                        unlock_table_id,
                        unlock_owner,
                        true,
                        unlock_tx_desc);
  prepare_out_trans_tx_(lock_ls,
                        stale_lock_table_id,
                        stale_lock_owner,
                        false,
                        stale_lock_tx_desc);
  prepare_out_trans_tx_(lock_ls,
                        unresolved_lock_table_id,
                        unresolved_lock_owner,
                        false,
                        unresolved_lock_tx_desc);

  ObSEArray<ExpectedTxCtxSnapshot, 3> expected_snapshots;
  ASSERT_EQ(OB_SUCCESS,
            expected_snapshots.push_back(ExpectedTxCtxSnapshot(
                unlock_tx_desc->tid(), unlock_owner, OUT_TRANS_UNLOCK)));
  ASSERT_EQ(OB_SUCCESS,
            expected_snapshots.push_back(ExpectedTxCtxSnapshot(
                stale_lock_tx_desc->tid(), stale_lock_owner, OUT_TRANS_LOCK)));
  ASSERT_EQ(OB_SUCCESS,
            expected_snapshots.push_back(ExpectedTxCtxSnapshot(
                unresolved_lock_tx_desc->tid(), unresolved_lock_owner, OUT_TRANS_LOCK)));

  // Checkpoint all active transactions together. A second tx ctx checkpoint
  // after either commit would omit that exited context and hide its stale
  // OUT_TRANS/DOING recovery path.
  capture_out_trans_tx_ctx_snapshots_(
      lock_ls, "out_trans_lock_snapshots", expected_snapshots);

  // Persist COMMIT_INFO after the tx ctx checkpoint, but deliberately leave
  // this transaction in REDO_COMPLETE without a final commit or abort log.
  // Restart must replay COMMIT_INFO and reconstruct its lock at start-working.
  SCN unresolved_commit_info_scn = SCN::min_scn();
  ASSERT_EQ(OB_SUCCESS,
            submit_commit_info_and_wait_(lock_ls,
                                         unresolved_lock_tx_desc->tid(),
                                         unresolved_commit_info_scn));

  SCN unlock_commit_scn = SCN::min_scn();
  commit_out_trans_tx_(unlock_tx_desc);
  wait_for_new_lock_commit_(lock_memtable, initial_lock_commit_scn, unlock_commit_scn);
  ASSERT_GT(unlock_commit_scn, unresolved_commit_info_scn);

  SCN stale_lock_commit_scn = SCN::min_scn();
  commit_out_trans_tx_(stale_lock_tx_desc);
  wait_for_new_lock_commit_(lock_memtable, unlock_commit_scn, stale_lock_commit_scn);
  ASSERT_GT(stale_lock_commit_scn, unlock_commit_scn);

  // Persist both final states. Keep the pre-commit tx ctx SSTable as the only
  // tx ctx checkpoint, so it remains recoverable after restart.
  ASSERT_EQ(OB_SUCCESS, flush_lock_table_(lock_memtable, stale_lock_commit_scn));
  const SCN stale_lock_flushed_scn = lock_memtable->flushed_scn_;
  ASSERT_GE(stale_lock_flushed_scn, stale_lock_commit_scn);
  ASSERT_GT(stale_lock_flushed_scn, unresolved_commit_info_scn);

  // The snapshot's rec_scn intentionally keeps K below COMMIT_INFO and both
  // final logs. Restart replays all three logs. F suppresses duplicate terminal
  // work, while the unresolved transaction must be reconstructed later at the
  // start-working SCN rather than at its older COMMIT_INFO SCN.
  const SCN clog_checkpoint_scn = lock_ls->get_clog_checkpoint_scn();
  ASSERT_LT(clog_checkpoint_scn, unresolved_commit_info_scn);
  ASSERT_LT(clog_checkpoint_scn, unlock_commit_scn);
  ASSERT_LT(clog_checkpoint_scn, stale_lock_commit_scn);

  // max_committed_scn_ was reset by the previous lock-table flush. Create a
  // later normal lock, then persist it as the final durable state.
  SCN final_lock_commit_scn = SCN::min_scn();
  ASSERT_EQ(OB_SUCCESS,
            table_lock_service->lock_table(unlock_table_id, EXCLUSIVE, final_lock_owner));
  wait_for_new_lock_commit_(lock_memtable, stale_lock_flushed_scn, final_lock_commit_scn);
  ASSERT_GT(final_lock_commit_scn, stale_lock_flushed_scn);

  ASSERT_EQ(OB_SUCCESS, flush_lock_table_(lock_memtable, final_lock_commit_scn));
  ASSERT_GE(lock_memtable->flushed_scn_, final_lock_commit_scn);
  bool found_unresolved_lock = false;
  ASSERT_EQ(OB_SUCCESS,
            has_lock_op_status_(lock_memtable,
                                unresolved_lock_table_id,
                                unresolved_lock_owner,
                                OUT_TRANS_LOCK,
                                LOCK_OP_DOING,
                                found_unresolved_lock));
  ASSERT_TRUE(found_unresolved_lock);
  LOG_INFO("constructed out trans lock recovery checkpoint skew",
           K(initial_lock_commit_scn),
           K(unresolved_commit_info_scn),
           K(unlock_commit_scn),
           K(stale_lock_commit_scn),
           K(stale_lock_flushed_scn),
           K(clog_checkpoint_scn),
           K(final_lock_commit_scn),
           "final_lock_flushed_scn",
           lock_memtable->flushed_scn_,
           "expected_order",
           "K < COMMIT_INFO_unresolved < C_unlock < C_stale_lock "
           "<= F_stale < C_final <= F_final");
}

class ObOutTransLockAfterRestartTest : public ObSimpleClusterTestBase
{
public:
  ObOutTransLockAfterRestartTest()
    : ObSimpleClusterTestBase(TEST_FILE_NAME)
  {}

protected:
  struct LockOpStatusCounts
  {
    LockOpStatusCounts() : doing_count_(0), complete_count_(0) {}

    int64_t doing_count_;
    int64_t complete_count_;
  };

  void get_table_id_(const uint64_t tenant_id,
                     const char *table_name,
                     uint64_t &table_id)
  {
    ObSchemaGetterGuard schema_guard;
    ASSERT_EQ(OB_SUCCESS,
              GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
    ASSERT_EQ(OB_SUCCESS,
              schema_guard.get_table_id(tenant_id,
                                        "test",
                                        table_name,
                                        false,
                                        share::schema::ObSchemaGetterGuard::NON_TEMP_WITH_NON_HIDDEN_TABLE_TYPE,
                                        table_id));
  }

  int get_lock_op_status_counts_(ObLockMemtable *lock_memtable,
                                 const uint64_t table_id,
                                 const ObTableLockOwnerID &owner_id,
                                 const ObTableLockOpType op_type,
                                 LockOpStatusCounts &counts)
  {
    int ret = OB_SUCCESS;
    ObLockID lock_id;
    ObLockOpIterator op_iter;
    transaction::tablelock::ObTableLockOp lock_op;

    counts = LockOpStatusCounts();
    if (OB_ISNULL(lock_memtable)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("lock memtable is null", K(ret), K(table_id));
    } else if (OB_FAIL(get_lock_id(table_id, lock_id))) {
      LOG_WARN("get lock id failed", K(ret), K(table_id));
    } else if (OB_FAIL(lock_memtable->get_lock_op_iter(lock_id, op_iter))) {
      LOG_WARN("get lock op iterator failed", K(ret), K(lock_id));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(op_iter.get_next(lock_op))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("iterate lock op failed", K(ret), K(lock_id));
          }
        } else if (owner_id == lock_op.owner_id_ && op_type == lock_op.op_type_) {
          if (LOCK_OP_DOING == lock_op.lock_op_status_) {
            ++counts.doing_count_;
          } else if (LOCK_OP_COMPLETE == lock_op.lock_op_status_) {
            ++counts.complete_count_;
          }
        }
      }
    }
    return ret;
  }

  int has_doing_out_trans_op_(ObLockMemtable *lock_memtable,
                              const ObTableLockOwnerID &owner_id,
                              const ObTableLockOpType op_type,
                              bool &found)
  {
    int ret = OB_SUCCESS;
    ObLockIDIterator lock_id_iter;
    found = false;
    if (OB_ISNULL(lock_memtable)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("lock memtable is null", K(ret));
    } else if (OB_FAIL(lock_memtable->get_lock_id_iter(lock_id_iter))) {
      LOG_WARN("get lock id iterator failed", K(ret));
    } else {
      ObLockID lock_id;
      while (!found && OB_SUCC(ret)) {
        if (OB_FAIL(lock_id_iter.get_next(lock_id))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("iterate lock id failed", K(ret));
          }
        } else {
          ObLockOpIterator op_iter;
          if (OB_FAIL(lock_memtable->get_lock_op_iter(lock_id, op_iter))) {
            LOG_WARN("get lock op iterator failed", K(ret), K(lock_id));
          }
          transaction::tablelock::ObTableLockOp lock_op;
          while (!found && OB_SUCC(ret)) {
            if (OB_FAIL(op_iter.get_next(lock_op))) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("iterate lock op failed", K(ret), K(lock_id));
              }
            } else {
              found = owner_id == lock_op.owner_id_
                   && op_type == lock_op.op_type_
                   && LOCK_OP_DOING == lock_op.lock_op_status_;
            }
          }
        }
      }
    }
    return ret;
  }

  int has_doing_out_trans_op_on_any_ls_(const ObTableLockOwnerID &owner_id,
                                        const ObTableLockOpType op_type,
                                        bool &found)
  {
    int ret = OB_SUCCESS;
    ObLSService *ls_service = MTL(ObLSService *);
    common::ObSharedGuard<ObLSIterator> ls_iter;
    found = false;
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", K(ret));
    } else if (OB_FAIL(ls_service->get_ls_iter(ls_iter, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("get ls iterator failed", K(ret));
    } else {
      ObLS *ls = nullptr;
      while (!found && OB_SUCC(ret)) {
        if (OB_FAIL(ls_iter->get_next(ls))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("iterate ls failed", K(ret));
          }
        } else if (OB_ISNULL(ls)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls is null", K(ret));
        } else {
          ObTableHandleV2 table_handle;
          ObLockMemtable *lock_memtable = nullptr;
          if (OB_FAIL(ls->lock_table_.get_lock_memtable(table_handle))) {
            LOG_WARN("get lock memtable handle failed", K(ret), "ls_id", ls->get_ls_id());
          } else if (OB_FAIL(table_handle.get_lock_memtable(lock_memtable))) {
            LOG_WARN("get lock memtable failed", K(ret), "ls_id", ls->get_ls_id());
          } else if (OB_ISNULL(lock_memtable)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("lock memtable is null", K(ret), "ls_id", ls->get_ls_id());
          } else if (OB_FAIL(has_doing_out_trans_op_(
                         lock_memtable, owner_id, op_type, found))) {
            LOG_WARN("scan doing out trans op failed",
                     K(ret), K(owner_id), K(op_type), "ls_id", ls->get_ls_id());
          }
        }
      }
    }
    return ret;
  }
};

TEST_F(ObOutTransLockAfterRestartTest, verify_recovered_lock_state)
{
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  uint64_t unlock_table_id = 0;
  uint64_t stale_lock_table_id = 0;
  uint64_t unresolved_lock_table_id = 0;
  get_table_id_(tenant_id, "out_trans_lock_recovery_t", unlock_table_id);
  get_table_id_(tenant_id, "out_trans_lock_snapshot_t", stale_lock_table_id);
  get_table_id_(tenant_id, "out_trans_lock_unresolved_t", unresolved_lock_table_id);

  ObLSService *ls_service = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_service);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS,
            ls_service->get_ls(share::LOCK_SERVICE_LS,
                               ls_handle,
                               ObLSGetMod::TABLELOCK_MOD));
  ObLS *lock_ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, lock_ls);

  ObTableHandleV2 table_handle;
  ObLockMemtable *lock_memtable = nullptr;
  ASSERT_EQ(OB_SUCCESS, lock_ls->lock_table_.get_lock_memtable(table_handle));
  ASSERT_EQ(OB_SUCCESS, table_handle.get_lock_memtable(lock_memtable));
  ASSERT_NE(nullptr, lock_memtable);

  ObTableLockOwnerID unlock_owner;
  ObTableLockOwnerID final_lock_owner;
  ObTableLockOwnerID stale_lock_owner;
  ObTableLockOwnerID unresolved_lock_owner;
  ASSERT_EQ(OB_SUCCESS,
            unlock_owner.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE, 1001));
  ASSERT_EQ(OB_SUCCESS,
            final_lock_owner.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE, 1002));
  ASSERT_EQ(OB_SUCCESS,
            stale_lock_owner.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE, 1003));
  ASSERT_EQ(OB_SUCCESS,
            unresolved_lock_owner.convert_from_value(
                ObLockOwnerType::DEFAULT_OWNER_TYPE, 1004));

  LockOpStatusCounts old_lock_counts;
  LockOpStatusCounts unlock_counts;
  LockOpStatusCounts final_lock_counts;
  LockOpStatusCounts stale_lock_counts;
  LockOpStatusCounts unresolved_lock_counts;
  ASSERT_EQ(OB_SUCCESS,
            get_lock_op_status_counts_(lock_memtable,
                                       unlock_table_id,
                                       unlock_owner,
                                       OUT_TRANS_LOCK,
                                       old_lock_counts));
  ASSERT_EQ(OB_SUCCESS,
            get_lock_op_status_counts_(lock_memtable,
                                       unlock_table_id,
                                       unlock_owner,
                                       OUT_TRANS_UNLOCK,
                                       unlock_counts));
  ASSERT_EQ(OB_SUCCESS,
            get_lock_op_status_counts_(lock_memtable,
                                       unlock_table_id,
                                       final_lock_owner,
                                       OUT_TRANS_LOCK,
                                       final_lock_counts));
  ASSERT_EQ(OB_SUCCESS,
            get_lock_op_status_counts_(lock_memtable,
                                       stale_lock_table_id,
                                       stale_lock_owner,
                                       OUT_TRANS_LOCK,
                                       stale_lock_counts));
  ASSERT_EQ(OB_SUCCESS,
            get_lock_op_status_counts_(lock_memtable,
                                       unresolved_lock_table_id,
                                       unresolved_lock_owner,
                                       OUT_TRANS_LOCK,
                                       unresolved_lock_counts));

  ASSERT_EQ(0, old_lock_counts.doing_count_);
  ASSERT_EQ(0, old_lock_counts.complete_count_);
  ASSERT_EQ(0, unlock_counts.doing_count_);
  ASSERT_EQ(0, unlock_counts.complete_count_);
  ASSERT_EQ(0, final_lock_counts.doing_count_);
  ASSERT_EQ(1, final_lock_counts.complete_count_);
  ASSERT_EQ(0, stale_lock_counts.doing_count_);
  ASSERT_EQ(1, stale_lock_counts.complete_count_);
  // The takeover timeout task may finish this local REDO_COMPLETE transaction
  // before the restart test starts. Its lock must exist exactly once as either
  // the reconstructed DOING operation or the subsequently committed COMPLETE
  // operation.
  ASSERT_EQ(1,
            unresolved_lock_counts.doing_count_
                + unresolved_lock_counts.complete_count_);

  bool found_doing_out_trans_op = false;
  ASSERT_EQ(OB_SUCCESS,
            has_doing_out_trans_op_on_any_ls_(
                unlock_owner, OUT_TRANS_LOCK, found_doing_out_trans_op));
  ASSERT_FALSE(found_doing_out_trans_op);
  ASSERT_EQ(OB_SUCCESS,
            has_doing_out_trans_op_on_any_ls_(
                unlock_owner, OUT_TRANS_UNLOCK, found_doing_out_trans_op));
  ASSERT_FALSE(found_doing_out_trans_op);
  ASSERT_EQ(OB_SUCCESS,
            has_doing_out_trans_op_on_any_ls_(
                final_lock_owner, OUT_TRANS_LOCK, found_doing_out_trans_op));
  ASSERT_FALSE(found_doing_out_trans_op);
  ASSERT_EQ(OB_SUCCESS,
            has_doing_out_trans_op_on_any_ls_(
                stale_lock_owner, OUT_TRANS_LOCK, found_doing_out_trans_op));
  ASSERT_FALSE(found_doing_out_trans_op);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  std::string gtest_file_name = std::string(TEST_FILE_NAME) + "_gtest.log";
  oceanbase::unittest::init_gtest_output(gtest_file_name);
  ObSimpleServerRestartHelper restart_helper(argc,
                                             argv,
                                             TEST_FILE_NAME,
                                             BORN_CASE_NAME,
                                             RESTART_CASE_NAME);
  restart_helper.set_sleep_sec(10);
  return restart_helper.run();
}
