/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TEST_TABLET_HELPER
#define OCEANBASE_STORAGE_TEST_TABLET_HELPER

#define private public
#define protected public

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_rpc_struct.h"
#include "share/scn.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/init_basic_struct.h"
#include "share/ob_upgrade_utils.h"
#include "common/ob_version_def.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/mds_table/ob_single_log_mds_ctx.h"
#include "storage/mds_table/ob_mds_table.h"
#include "storage/mds_table/ob_mds_table_scan_iterator.h"
#include "storage/mds_table/ob_mds_tablet_schema.h"
#include "storage/mds_table/ob_mds_type_id_map.h"
#include "storage/mds_table/ob_mds_type_registry.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "storage/tablet/ob_i_tablet_mds_interface.h"
#include "storage/ob_i_store.h"
#include "storage/memtable/mvcc/ob_mvcc_acc_ctx.h"
#include "storage/memtable/ob_concurrent_control.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_ctx_mgr_v4.h"

namespace oceanbase
{
namespace common
{
// for unittest, return the current data version
int ObClusterVersion::get_tenant_data_version(const uint64_t tenant_id, uint64_t &data_version)
{
  data_version = DATA_CURRENT_VERSION;
  return OB_SUCCESS;
}
}
namespace storage
{
class TestTabletHelper
{
public:
  // How create_tablet leaves the tablet's "MDS write" footprint after seeding status.
  //   MDS_REPLAY: write committed status via replay_set_tablet_status and leave
  //               ObTabletPointer::tablet_mds_status_written_ = true (the natural
  //               state of a tablet whose status was written on this server).
  //   MIGRATED:   same MDS write, then reset_tablet_status_written() so the
  //               written flag is false — mimics a tablet brought in by migration
  //               where the committed status came from the source and was never
  //               produced by this server's MDS write path.
  enum class StatusInjectMode
  {
    MDS_REPLAY = 0,
    MIGRATED   = 1,
  };
  static void prepare_sstable_param(
      const common::ObTabletID &tablet_id,
      const share::schema::ObTableSchema &table_schema,
      ObTabletCreateSSTableParam &param);
  static int create_tablet(
      ObLSHandle &ls_handle,
      const common::ObTabletID &tablet_id,
      const share::schema::ObTableSchema &table_schema,
      common::ObArenaAllocator &allocator,
      const ObTabletStatus::Status tablet_status,
      const share::SCN &create_commit_scn,
      ObTabletHandle &handle,
      const StatusInjectMode inject_mode = StatusInjectMode::MDS_REPLAY);
  static int create_tablet(
      ObLSHandle &ls_handle,
      const common::ObTabletID &tablet_id,
      const share::schema::ObTableSchema &table_schema,
      common::ObArenaAllocator &allocator,
      const ObTabletStatus::Status tablet_status = ObTabletStatus::NORMAL,
      const share::SCN &create_commit_scn = share::SCN::min_scn(),
      const StatusInjectMode inject_mode = StatusInjectMode::MDS_REPLAY);
  static int remove_tablet(
      const ObLSHandle &ls_handle,
      const common::ObTabletID &tablet_id);
  // Legacy: binds full ObMemtableCtx + tx_table write path. Plain MdsCtx::single_log_commit()
  // only advances 2PC state and does NOT commit LS aggregate MDS rows — use UtSingleLogMdsWriteCtx.
  // @p mem_ctx and @p store_ctx must outlive all MDS set/replay calls on @p ctx.
  static int prepare_mds_write_ctx_for_test(
      ObLS &ls,
      ObTablet &tablet,
      const common::ObTabletID &tablet_id,
      const transaction::ObTransID &tx_id,
      mds::MdsCtx &ctx,
      memtable::ObMemtableCtx &mem_ctx,
      ObStoreCtx &store_ctx);
  // One-shot LS aggregate MDS write + commit for tablet status (ctx owned internally).
  static int write_tablet_status_for_test(
      ObLS &ls,
      ObTablet &tablet,
      const common::ObTabletID &tablet_id,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const share::SCN &redo_scn,
      const share::SCN &commit_scn,
      const transaction::ObTransID &tx_id);
};

inline void TestTabletHelper::prepare_sstable_param(
    const common::ObTabletID &tablet_id,
    const share::schema::ObTableSchema &table_schema,
    ObTabletCreateSSTableParam &param)
{
  param.set_init_value_for_column_store_();
  const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  param.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  param.table_key_.tablet_id_ = tablet_id;
  param.table_key_.version_range_.base_version_ = ObVersionRange::MIN_VERSION;
  param.table_key_.version_range_.snapshot_version_ = 1;
  param.schema_version_ = table_schema.get_schema_version();
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = table_schema.get_progressive_merge_round();
  param.progressive_merge_step_ = 0;
  param.table_mode_ = table_schema.get_table_mode_struct();
  param.index_type_ = table_schema.get_index_type();
  param.rowkey_column_cnt_ = table_schema.get_rowkey_column_num()
            + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 0;
  param.micro_block_cnt_ = 0;
  param.use_old_macro_block_count_ = 0;
  param.column_cnt_ = table_schema.get_column_count() + multi_version_col_cnt;
  param.data_checksum_ = 0;
  param.occupy_size_ = 0;
  param.ddl_scn_.set_min();
  param.filled_tx_scn_.set_min();
  param.tx_data_recycle_scn_.set_min();
  param.rec_scn_.set_min();
  param.original_size_ = 0;
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.encrypt_id_ = 0;
  param.master_key_id_ = 0;
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.recycle_version_ = 0;
  param.row_count_ = 0;
  param.sstable_logic_seq_ = 0;
  param.nested_offset_ = 0;
  param.nested_size_ = 0;
}

inline int ensure_mds_type_registry_for_ut()
{
  int ret = OB_SUCCESS;
  static bool registered = false;
  if (!registered) {
    constexpr uint16_t tablet_status_type_id = static_cast<uint16_t>(
        mds_type_id_detail::TypeIdOf<mds::DummyKey, ObTabletCreateDeleteMdsUserData>::value);
    if (OB_NOT_NULL(ObMdsTypeRegistry::get_instance().get_ops(tablet_status_type_id))) {
      registered = true;
    } else if (OB_FAIL(ObMdsTypeRegistry::init_all_mds_types())) {
      STORAGE_LOG(WARN, "failed to init mds type registry for ut", K(ret));
    } else {
      registered = true;
    }
  }
  return ret;
}

inline int ensure_mds_tablet_schema_helper_for_ut()
{
  int ret = OB_SUCCESS;
  ObMdsTabletSchemaHelper &helper = ObMdsTabletSchemaHelper::get_instance();
  if (!helper.is_inited()) {
    if (OB_FAIL(helper.init())) {
      STORAGE_LOG(WARN, "failed to init mds tablet schema helper for ut", K(ret));
    }
  }
  return ret;
}

inline int get_ls_mds_tablet_handle_for_ut(const ObITabletMdsInterface &tablet_mds,
                                           ObTabletHandle &out_handle)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_mds.get_tablet_meta_().ls_id_;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls service is null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::TABLET_MOD))) {
    STORAGE_LOG(WARN, "fail to get ls for ls mds tablet read", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret), K(ls_id));
  } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(LS_MDS_TABLET, out_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get ls mds inner tablet", K(ret), K(ls_id));
    }
  }
  return ret;
}

inline int init_mds_scan_iterator_for_ut(const ObITabletMdsInterface &tablet_mds,
                                         ObTableScanParam &scan_param,
                                         ObStoreCtx &store_ctx,
                                         ObMdsTableScanIterator &iter)
{
  int ret = OB_SUCCESS;
  ObTabletHandle ls_mds_handle;
  share::SCN read_snapshot;
  if (OB_UNLIKELY(!ObMdsTabletSchemaHelper::get_instance().is_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "mds tablet schema helper not inited", K(ret));
  } else if (OB_FAIL(read_snapshot.convert_for_tx(scan_param.read_version_range_.snapshot_version_))) {
    STORAGE_LOG(WARN, "failed to convert read snapshot for mds scan", K(ret), K(scan_param.read_version_range_));
  } else if (!store_ctx.is_valid()
             && OB_FAIL(store_ctx.init_for_read(
                    scan_param.ls_id_, LS_MDS_TABLET, scan_param.timeout_, -1, read_snapshot))) {
    STORAGE_LOG(WARN, "failed to init store ctx for mds scan", K(ret), K(scan_param.ls_id_), K(read_snapshot));
  } else if (OB_FAIL(get_ls_mds_tablet_handle_for_ut(tablet_mds, ls_mds_handle))) {
    STORAGE_LOG(WARN, "failed to get ls mds tablet handle for read", K(ret));
  } else if (OB_FAIL(iter.init(scan_param, ls_mds_handle, store_ctx))) {
    STORAGE_LOG(WARN, "failed to init mds table scan iterator", K(ret));
  }
  return ret;
}

// Scan LS_MDS_TABLET for one keyed MDS row (supports read_uncommitted; used when point-get path fails in UT).
template <typename Key, typename Value>
inline int ut_read_mds_key_via_scan(
    common::ObIAllocator &allocator,
    const ObITabletMdsInterface &tablet_mds,
    const share::ObLSID &ls_id,
    const common::ObTabletID &user_tablet_id,
    const share::SCN &transfer_scn,
    const Key &key,
    const int64_t snapshot_version,
    const int64_t timeout_us,
    const bool read_uncommitted,
    Value &value,
    ObMdsValueMeta &meta)
{
  int ret = OB_SUCCESS;
  char key_buf[ObMdsTabletSchemaDef::MDS_KEY_COLUMN_LENGTH];
  int64_t pos = 0;
  if (OB_FAIL(ensure_mds_tablet_schema_helper_for_ut())) {
    STORAGE_LOG(WARN, "failed to ensure mds tablet schema for ut scan read", K(ret));
  } else if (OB_FAIL(key.mds_serialize(key_buf, static_cast<int64_t>(sizeof(key_buf)), pos))) {
    STORAGE_LOG(WARN, "failed to serialize mds key for ut scan", K(ret));
  } else {
    constexpr uint8_t mds_unit_id = static_cast<uint8_t>(mds_type_id_detail::TypeIdOf<Key, Value>::value);
    ObTableScanParam scan_param;
    ObStoreCtx store_ctx;
    ObMdsTableScanIterator iter;
    Key read_key;
    if (OB_FAIL(ObMdsScanParamHelper::build_mds_unit_range_scan_param(
            allocator, ls_id, user_tablet_id, transfer_scn, mds_unit_id,
            timeout_us, ObVersionRange(0, snapshot_version),
            false/*is_select_follower*/, scan_param))) {
      STORAGE_LOG(WARN, "failed to build mds scan param for ut", K(ret), K(user_tablet_id), K(key));
    } else {
      if (read_uncommitted) {
        scan_param.scan_flag_.set_iter_uncommitted_row();
      }
      if (OB_FAIL(init_mds_scan_iterator_for_ut(tablet_mds, scan_param, store_ctx, iter))) {
        STORAGE_LOG(WARN, "failed to init mds scan iterator for ut", K(ret), K(user_tablet_id), K(key));
      } else {
        while (OB_SUCC(ret)) {
          read_key.reset();
          value.reset();
          meta.reset();
          if (OB_FAIL(iter.get_next_mds_kv(allocator, read_key, value, meta))) {
            if (OB_ITER_END == ret) {
              ret = OB_EMPTY_RESULT;
            }
          } else {
            char read_key_buf[ObMdsTabletSchemaDef::MDS_KEY_COLUMN_LENGTH];
            int64_t read_pos = 0;
            if (OB_FAIL(read_key.mds_serialize(read_key_buf, static_cast<int64_t>(sizeof(read_key_buf)), read_pos))) {
              STORAGE_LOG(WARN, "failed to serialize read key", K(ret));
            } else if (read_pos == pos && 0 == MEMCMP(key_buf, read_key_buf, pos)) {
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

inline int ensure_ls_mds_tablet_memtable_ready(ObLS &ls)
{
  int ret = OB_SUCCESS;
  ObTabletHandle mds_tablet_hdl;
  ObLSTabletService *ls_tablet_svr = ls.get_tablet_svr();
  if (OB_ISNULL(ls_tablet_svr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls tablet service is null", K(ret), K(ls.get_ls_id()));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_MDS_TABLET, mds_tablet_hdl))) {
    if (OB_TABLET_NOT_EXIST == ret && OB_NOT_NULL(ls.get_mds_table())) {
      if (OB_FAIL(ls.get_mds_table()->create_tablet(share::SCN::base_scn()))) {
        STORAGE_LOG(WARN, "failed to create ls mds tablet", K(ret), K(ls.get_ls_id()));
      } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_MDS_TABLET, mds_tablet_hdl))) {
        STORAGE_LOG(WARN, "failed to get ls mds tablet after create", K(ret), K(ls.get_ls_id()));
      }
    } else if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "failed to get ls mds tablet", K(ret), K(ls.get_ls_id()));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(mds_tablet_hdl.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls mds tablet is null", K(ret), K(ls.get_ls_id()));
  } else {
    const share::SCN ckpt = mds_tablet_hdl.get_obj()->get_tablet_meta().clog_checkpoint_scn_;
    CreateMemtableArg arg;
    ObSEArray<ObITable *, 8> memtables;
    const share::schema::ObTableSchema *mds_schema =
        ObMdsTabletSchemaHelper::get_instance().get_table_schema();
    arg.for_replay_ = true;
    arg.clog_checkpoint_scn_ = ckpt;
    if (OB_NOT_NULL(mds_schema)) {
      arg.schema_version_ = mds_schema->get_schema_version();
    }
    if (OB_FAIL(ls_tablet_svr->create_memtable(LS_MDS_TABLET, arg))) {
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "failed to create ls mds tablet memtable", K(ret), K(ls.get_ls_id()), K(ckpt));
      }
    } else if (OB_FAIL(mds_tablet_hdl.get_obj()->inner_get_memtables(memtables))) {
      STORAGE_LOG(WARN, "failed to get ls mds tablet memtables", K(ret), K(ls.get_ls_id()));
    } else if (memtables.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls mds tablet has no active memtable after create", K(ret), K(ls.get_ls_id()), K(ckpt));
    }
  }
  return ret;
}

struct UtMdsWriteEnv
{
  memtable::ObMemtableCtx mem_ctx;
  ObStoreCtx store_ctx;
};

inline UtMdsWriteEnv &ut_mds_write_env()
{
  thread_local UtMdsWriteEnv env;
  return env;
}

// Opens a REAL ObPartTransCtx for UT MDS writes so the row's tx state can be
// resolved via ObTransService (no more OB_TRANS_CTX_NOT_EXIST when readers
// walk uncommitted MDS rows). Driven by the production acquire_tx + start_tx
// + get_write_store_ctx path so the store_ctx, mvcc_acc_ctx, mem_ctx and
// part_ctx are all wired the same way as the real write side.
//
// Usage:
//   UtMdsTxScope scope(ls);
//   ASSERT_EQ(OB_SUCCESS, scope.begin(tablet_id));   // 0-arg ok: random tx_id
//   // hand scope.store_ctx() / scope.mem_ctx() to MdsCtx / set_tablet_status
//   ...
//   scope.end();   // or let dtor revert
class UtMdsTxScope final
{
public:
  explicit UtMdsTxScope(ObLS &ls, const int64_t trans_expire_us = 0)
    : ls_(&ls), expire_us_(trans_expire_us),
      tx_service_(nullptr), tx_desc_(nullptr), part_ctx_(nullptr),
      store_ctx_bound_(false), tx_started_(false) {}
  ~UtMdsTxScope() { end(); }

  // Open the transaction and bind a write store_ctx on @p tablet_id.
  int begin(const common::ObTabletID &tablet_id)
  {
    int ret = OB_SUCCESS;
    tx_service_ = MTL(transaction::ObTransService*);
    if (OB_ISNULL(tx_service_) || OB_ISNULL(ls_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "tx_service or ls is null", K(ret));
    } else if (OB_FAIL(tx_service_->acquire_tx(tx_desc_, 100 /*session_id*/))) {
      STORAGE_LOG(WARN, "acquire_tx failed in UT scope", K(ret));
    } else {
      transaction::ObTxParam tx_p;
      tx_p.timeout_us_ = ObTimeUtility::current_time() + 100L * 1000L * 1000L;
      tx_p.access_mode_ = transaction::ObTxAccessMode::RW;
      tx_p.isolation_ = transaction::ObTxIsolationLevel::RC;
      tx_p.cluster_id_ = 1;
      if (OB_FAIL(tx_service_->start_tx(*tx_desc_, tx_p))) {
        STORAGE_LOG(WARN, "start_tx failed in UT scope", K(ret));
      } else {
        tx_started_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      transaction::ObTxIsolationLevel iso = transaction::ObTxIsolationLevel::RC;
      const int64_t expire_ts = (expire_us_ > 0)
          ? expire_us_
          : ObTimeUtility::current_time() + 5L * 1000L * 1000L;
      if (OB_FAIL(tx_service_->get_read_snapshot(*tx_desc_, iso, expire_ts, snapshot_))) {
        STORAGE_LOG(WARN, "get_read_snapshot failed in UT scope", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      concurrent_control::ObWriteFlag write_flag;
      write_flag.set_is_mds();
      store_ctx_.ls_id_ = ls_->get_ls_id();
      store_ctx_.ls_ = ls_;
      store_ctx_.tablet_id_ = tablet_id;
      store_ctx_.timeout_ = ObTimeUtility::current_time() + 5L * 1000L * 1000L;
      if (OB_FAIL(tx_service_->get_write_store_ctx(*tx_desc_, snapshot_, write_flag,
              store_ctx_, transaction::ObTxSEQ::MIN_VAL(), true /*special*/))) {
        STORAGE_LOG(WARN, "get_write_store_ctx failed in UT scope", K(ret));
      } else {
        part_ctx_ = store_ctx_.mvcc_acc_ctx_.tx_ctx_;
        store_ctx_bound_ = true;
      }
    }
    return ret;
  }

  // Hand-off helpers to attach to an MdsCtx exactly like production register_mds_into_ctx_.
  void bind_to_mds_ctx(mds::MdsCtx &mds_ctx)
  {
    mds_ctx.set_mem_ctx_for_replay(*part_ctx_->get_memtable_ctx());
    mds_ctx.set_store_ctx_for_write(store_ctx_);
  }

  void end()
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(tx_service_)) {
      // never began
      return;
    }
    if (store_ctx_bound_) {
      if (OB_FAIL(tx_service_->revert_store_ctx(store_ctx_))) {
        STORAGE_LOG(WARN, "revert_store_ctx failed in UT scope", K(ret));
      }
      store_ctx_.reset();
      store_ctx_bound_ = false;
      part_ctx_ = nullptr;
    }
    if (OB_NOT_NULL(tx_desc_)) {
      if (tx_started_) {
        // Best-effort rollback so the part_ctx tears down cleanly.
        const int tmp = tx_service_->rollback_tx(*tx_desc_);
        if (OB_SUCCESS != tmp) {
          ret = tmp;
          STORAGE_LOG(WARN, "rollback_tx failed in UT scope", K(ret));
        }
        tx_started_ = false;
      }
      tx_service_->release_tx(*tx_desc_);
      tx_desc_ = nullptr;
    }
  }

  transaction::ObPartTransCtx *part_ctx() { return part_ctx_; }
  memtable::ObMemtableCtx *mem_ctx() { return part_ctx_ ? part_ctx_->get_memtable_ctx() : nullptr; }
  ObStoreCtx &store_ctx() { return store_ctx_; }
  transaction::ObTxDesc *tx_desc() { return tx_desc_; }

private:
  ObLS *ls_;
  int64_t expire_us_;
  transaction::ObTransService *tx_service_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot snapshot_;
  transaction::ObPartTransCtx *part_ctx_;
  ObStoreCtx store_ctx_;
  bool store_ctx_bound_;
  bool tx_started_;
};

// Single-phase MDS write/commit for LS_MDS_TABLET aggregate path (unit/mittest).
// Wraps production SingleLogMdsCtx + MdsSingleRowMvccCtx — no ObTransService / part_ctx.
class UtSingleLogMdsWriteCtx final
{
public:
  explicit UtSingleLogMdsWriteCtx(const transaction::ObTransID &tx_id)
    : ctx_(mds::MdsWriter(tx_id)) {}
  explicit UtSingleLogMdsWriteCtx(const mds::MdsWriter &writer)
    : ctx_(writer) {}

  int prepare(ObLS &ls, const int64_t abs_lock_timeout = INT64_MAX)
  {
    return ctx_.init(ls.get_ls_id(), abs_lock_timeout, false /*is_replay*/);
  }

  void commit(const share::SCN &commit_scn)
  {
    ctx_.single_log_commit(commit_scn, commit_scn);
  }

  void abort() { ctx_.single_log_abort(); }

  mds::SingleLogMdsCtx &single_log_ctx() { return ctx_; }
  mds::MdsCtx &mds_ctx() { return ctx_; }
  operator mds::MdsCtx &() { return ctx_; }

private:
  mds::SingleLogMdsCtx ctx_;
};

inline int prepare_single_log_mds_write_ctx_for_test(
    ObLS &ls,
    UtSingleLogMdsWriteCtx &ctx,
    const int64_t abs_lock_timeout = INT64_MAX)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ensure_mds_type_registry_for_ut())) {
    STORAGE_LOG(WARN, "failed to ensure mds type registry for single-log mds write", K(ret));
  } else if (OB_FAIL(ensure_ls_mds_tablet_memtable_ready(ls))) {
    STORAGE_LOG(WARN, "failed to ensure ls mds tablet memtable for single-log write", K(ret), K(ls.get_ls_id()));
  } else if (OB_FAIL(ctx.prepare(ls, abs_lock_timeout))) {
    STORAGE_LOG(WARN, "failed to prepare UtSingleLogMdsWriteCtx", K(ret), K(ls.get_ls_id()));
  }
  return ret;
}

// Multi-key / multi-write LS aggregate MDS path: ObMemtableCtx supports multiple row callbacks.
// Multi-key write path: on_redo -> trans_end -> on_commit (MVCC tx_commit).
class UtMemtableMdsWriteCtx final
{
public:
  explicit UtMemtableMdsWriteCtx(const transaction::ObTransID &tx_id)
    : mds_ctx_(mds::MdsWriter(tx_id)) {}
  explicit UtMemtableMdsWriteCtx(const mds::MdsWriter &writer)
    : mds_ctx_(writer) {}

  int prepare(ObLS &ls,
              ObTablet &tablet,
              const common::ObTabletID &tablet_id,
              const int64_t abs_lock_timeout = INT64_MAX)
  {
    return TestTabletHelper::prepare_mds_write_ctx_for_test(
        ls, tablet, tablet_id, transaction::ObTransID(mds_ctx_.get_writer().writer_id_),
        mds_ctx_, mem_ctx_, store_ctx_);
  }

  void commit(const share::SCN &commit_scn)
  {
    mds_ctx_.on_redo(commit_scn);
    int ret = OB_SUCCESS;
    if (OB_FAIL(mem_ctx_.trans_end(true, commit_scn, commit_scn))) {
      STORAGE_LOG(ERROR, "UtMemtableMdsWriteCtx commit trans_end failed", K(ret), K(commit_scn));
    }
    mds_ctx_.on_commit(commit_scn, commit_scn);
  }

  void abort()
  {
    const share::SCN abort_scn = share::SCN::max_scn();
    mds_ctx_.on_abort(abort_scn);
    int ret = OB_SUCCESS;
    if (OB_FAIL(mem_ctx_.trans_end(false, abort_scn, abort_scn))) {
      STORAGE_LOG(ERROR, "UtMemtableMdsWriteCtx abort trans_end failed", K(ret));
    }
  }

  mds::MdsCtx &mds_ctx() { return mds_ctx_; }
  operator mds::MdsCtx &() { return mds_ctx_; }

private:
  mds::MdsCtx mds_ctx_;
  memtable::ObMemtableCtx mem_ctx_;
  ObStoreCtx store_ctx_;
};

inline int prepare_memtable_mds_write_ctx_for_test(
    ObLS &ls,
    ObTablet &tablet,
    const common::ObTabletID &tablet_id,
    UtMemtableMdsWriteCtx &ctx,
    const int64_t abs_lock_timeout = INT64_MAX)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ensure_mds_type_registry_for_ut())) {
    STORAGE_LOG(WARN, "failed to ensure mds type registry for memtable mds write", K(ret));
  } else if (OB_FAIL(ensure_mds_tablet_schema_helper_for_ut())) {
    STORAGE_LOG(WARN, "failed to ensure mds tablet schema for memtable mds write", K(ret));
  } else if (OB_FAIL(ensure_ls_mds_tablet_memtable_ready(ls))) {
    STORAGE_LOG(WARN, "failed to ensure ls mds tablet memtable for memtable write", K(ret), K(ls.get_ls_id()));
  } else if (OB_FAIL(ctx.prepare(ls, tablet, tablet_id, abs_lock_timeout))) {
    STORAGE_LOG(WARN, "failed to prepare UtMemtableMdsWriteCtx", K(ret), K(ls.get_ls_id()), K(tablet_id));
  }
  return ret;
}

inline int TestTabletHelper::prepare_mds_write_ctx_for_test(
    ObLS &ls,
    ObTablet &tablet,
    const common::ObTabletID &tablet_id,
    const transaction::ObTransID &tx_id,
    mds::MdsCtx &ctx,
    memtable::ObMemtableCtx &mem_ctx,
    ObStoreCtx &store_ctx)
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id = ls.get_ls_id();
  if (mem_ctx.is_inited_) {
    mem_ctx.reset();
  }
  store_ctx.mvcc_acc_ctx_.reset();
  if (OB_NOT_NULL(ctx.get_store_ctx())) {
    // already prepared by caller
  } else if (OB_ISNULL(ls.get_tx_table())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx table is null for test mds write", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ensure_mds_type_registry_for_ut())) {
    STORAGE_LOG(WARN, "failed to ensure mds type registry for test write", K(ret), K(ls_id));
  } else if (OB_FAIL(ensure_ls_mds_tablet_memtable_ready(ls))) {
    STORAGE_LOG(WARN, "failed to ensure ls mds tablet memtable for test write", K(ret), K(ls_id));
  } else if (OB_FAIL(mem_ctx.init(
      common::OB_SYS_TENANT_ID,
      CLUSTER_CURRENT_VERSION,
      tx_id,
      ls_id,
      common::ObAddr()))) {
    STORAGE_LOG(WARN, "failed to init mem_ctx for test mds write", K(ret), K(ls_id), K(tablet_id), K(tx_id));
  } else {
    transaction::ObTxSnapshot snapshot;
    snapshot.version_ = share::SCN::max_scn();
    concurrent_control::ObWriteFlag write_flag;
    store_ctx.ls_id_ = ls_id;
    store_ctx.ls_ = &ls;
    store_ctx.tablet_id_ = tablet.get_tablet_meta().tablet_id_;
    if (OB_FAIL(store_ctx.mvcc_acc_ctx_.init_write_light(
        mem_ctx,
        mem_ctx.get_tx_id(),
        transaction::ObTxSEQ::MIN_VAL(),
        ls.get_tx_table(),
        snapshot,
        INT64_MAX,
        0,
        write_flag))) {
      STORAGE_LOG(WARN, "failed to init store ctx for test mds write", K(ret), K(ls_id), K(tablet_id), K(tx_id));
    } else {
      ctx.set_mem_ctx_for_replay(mem_ctx);
      ctx.set_store_ctx_for_write(store_ctx);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ensure_ls_mds_tablet_memtable_ready(ls))) {
    STORAGE_LOG(WARN, "failed to ensure ls mds tablet memtable", K(ret), K(ls_id));
  }
  return ret;
}

inline int ut_prepare_mds_write(
    const share::ObLSID &ls_id,
    ObTablet &tablet,
    const common::ObTabletID &tablet_id,
    const transaction::ObTransID &tx_id,
    mds::MdsCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  UtMdsWriteEnv &env = ut_mds_write_env();
  if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "failed to get ls for ut mds write", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null for ut mds write", K(ret), K(ls_id));
  } else if (OB_FAIL(TestTabletHelper::prepare_mds_write_ctx_for_test(
      *ls_handle.get_ls(), tablet, tablet_id, tx_id, ctx, env.mem_ctx, env.store_ctx))) {
    STORAGE_LOG(WARN, "failed to prepare ut mds write ctx", K(ret), K(ls_id), K(tablet_id), K(tx_id));
  }
  return ret;
}

// Align UT with production: NotifyType::TX_END -> MdsCtx::before_prepare + mt_ctx.before_prepare.
inline void ut_advance_mds_tx_before_prepare(
    mds::MdsCtx &ctx,
    memtable::ObMemtableCtx &mem_ctx,
    const share::SCN &prepare_version)
{
  ctx.before_prepare();
  mem_ctx.before_prepare(prepare_version);
}

// Align UT with production: NotifyType::ON_PREPARE -> MdsCtx::on_prepare + mt_ctx.set_prepare_version.
inline void ut_advance_mds_tx_on_prepare(
    mds::MdsCtx &ctx,
    memtable::ObMemtableCtx &mem_ctx,
    const share::SCN &prepare_version)
{
  ctx.on_prepare(prepare_version);
  mem_ctx.set_prepare_version(prepare_version);
}

// TRANSACTION writer: on_redo + trans_end + on_commit (replaces single_log_commit in UT).
inline int ut_finish_mds_tx_commit(
    mds::MdsCtx &ctx,
    memtable::ObMemtableCtx &mem_ctx,
    const share::SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  ctx.on_redo(commit_scn);
  if (OB_FAIL(mem_ctx.trans_end(true, commit_scn, commit_scn))) {
    STORAGE_LOG(WARN, "failed to trans_end for ut mds tx commit", K(ret), K(commit_scn));
  } else {
    ctx.on_commit(commit_scn, commit_scn);
  }
  return ret;
}

// TRANSACTION writer: trans_end + on_commit when on_redo was already invoked.
inline int ut_finish_mds_tx_commit_after_redo(
    mds::MdsCtx &ctx,
    memtable::ObMemtableCtx &mem_ctx,
    const share::SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mem_ctx.trans_end(true, commit_scn, commit_scn))) {
    STORAGE_LOG(WARN, "failed to trans_end for ut mds tx commit after redo", K(ret), K(commit_scn));
  } else {
    ctx.on_commit(commit_scn, commit_scn);
  }
  return ret;
}

inline int TestTabletHelper::write_tablet_status_for_test(
    ObLS &ls,
    ObTablet &tablet,
    const common::ObTabletID &tablet_id,
    const ObTabletCreateDeleteMdsUserData &user_data,
    const share::SCN &redo_scn,
    const share::SCN &commit_scn,
    const transaction::ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id = ls.get_ls_id();
  memtable::ObMemtableCtx mem_ctx;
  ObStoreCtx store_ctx;
  mds::MdsCtx ctx{mds::MdsWriter(tx_id)};
  if (OB_FAIL(prepare_mds_write_ctx_for_test(
      ls, tablet, tablet_id, tx_id, ctx, mem_ctx, store_ctx))) {
    STORAGE_LOG(WARN, "failed to prepare mds write ctx for test", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet.set_tablet_status(user_data, ctx))) {
    STORAGE_LOG(WARN, "failed to set tablet status for test", K(ret), K(ls_id), K(tablet_id), K(user_data));
  } else {
    ctx.on_redo(redo_scn);
    if (OB_FAIL(mem_ctx.trans_end(true, commit_scn, commit_scn))) {
      STORAGE_LOG(WARN, "failed to trans_end for test tablet status write", K(ret), K(commit_scn));
    } else {
      ctx.on_commit(commit_scn, commit_scn);
    }
  }
  return ret;
}

// LS aggregate MDS: replay_scn must be strictly greater than LS_MDS_TABLET clog_checkpoint.
inline int calc_test_tablet_mds_replay_scn(
    ObLS &ls,
    const share::SCN &create_commit_scn,
    share::SCN &replay_scn)
{
  int ret = OB_SUCCESS;
  share::SCN mds_ckpt_scn;
  ObMdsTable *mds_table = ls.get_mds_table();
  if (OB_ISNULL(mds_table)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "mds table is null", K(ret), K(ls.get_ls_id()));
  } else if (OB_FAIL(mds_table->get_mds_checkpoint_scn(mds_ckpt_scn))) {
    STORAGE_LOG(WARN, "failed to get mds tablet clog checkpoint scn", K(ret), K(ls.get_ls_id()));
  } else {
    replay_scn = share::SCN::plus(mds_ckpt_scn, 1);
    if (create_commit_scn.is_valid_and_not_min() && create_commit_scn > replay_scn) {
      replay_scn = create_commit_scn;
    }
  }
  return ret;
}

// Production replay commits via ObPartTransCtx::trans_replay_commit_ -> trans_replay_end.
// UT replay must do the same or MVCC nodes stay undecided and block later writers (-6003).
inline int ut_finish_memctx_mds_replay(memtable::ObMemtableCtx &mem_ctx, const share::SCN &replay_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mem_ctx.trans_replay_end(true /*commit*/, replay_scn, replay_scn))) {
    STORAGE_LOG(WARN, "failed to commit ut mds replay", K(ret), K(replay_scn));
  }
  return ret;
}

inline int build_test_create_tablet_mds_user_data(
    const ObTabletStatus::Status tablet_status,
    const share::SCN &create_commit_scn,
    const share::SCN &replay_scn,
    ObTabletCreateDeleteMdsUserData &user_data)
{
  int ret = OB_SUCCESS;
  user_data.reset();
  user_data.tablet_status_ = ObTabletStatus(tablet_status);
  user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
  if (create_commit_scn.is_valid_and_not_min()) {
    user_data.create_commit_scn_ = create_commit_scn;
    user_data.create_commit_version_ = create_commit_scn.get_val_for_tx();
  } else {
    user_data.create_commit_scn_ = replay_scn;
    user_data.create_commit_version_ = replay_scn.get_val_for_tx();
  }
  if (ObTabletStatus::Status::DELETED == tablet_status
      || ObTabletStatus::Status::TRANSFER_OUT_DELETED == tablet_status) {
    user_data.delete_commit_scn_ = share::SCN::plus(user_data.create_commit_scn_, 100);
    user_data.delete_commit_version_ = user_data.delete_commit_scn_.get_val_for_tx();
  }
  if (ObTabletStatus::Status::TRANSFER_OUT_DELETED == tablet_status) {
    // Use a valid but non-existent dest LS so check_transfer_out_deleted_tablet_
    // can proceed (OB_LS_NOT_EXIST => safe to become empty shell).
    user_data.transfer_ls_id_ = share::ObLSID(9999);
  }
  if (OB_UNLIKELY(!user_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid mds user data for test tablet", K(ret), K(user_data), K(tablet_status));
  }
  return ret;
}

// Seed LS aggregate MDS via replay (no ObStoreCtx); for mittest data setup.
inline int ut_replay_tablet_status_for_test(
    ObLS &ls,
    const common::ObTabletID &tablet_id,
    const ObTabletCreateDeleteMdsUserData &user_data,
    const share::SCN &replay_scn_in,
    const transaction::ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  share::SCN replay_scn = replay_scn_in;
  memtable::ObMemtableCtx mem_ctx;
  mds::MdsCtx ctx{mds::MdsWriter(tx_id)};
  if (OB_FAIL(ensure_mds_type_registry_for_ut())) {
    STORAGE_LOG(WARN, "failed to ensure mds type registry", K(ret));
  } else if (OB_FAIL(ensure_ls_mds_tablet_memtable_ready(ls))) {
    STORAGE_LOG(WARN, "failed to ensure ls mds tablet memtable", K(ret), K(ls.get_ls_id()));
  } else if (OB_FAIL(calc_test_tablet_mds_replay_scn(ls, replay_scn_in, replay_scn))) {
    STORAGE_LOG(WARN, "failed to calc ut mds replay scn", K(ret), K(ls.get_ls_id()), K(tablet_id));
  } else if (OB_FAIL(mem_ctx.init(
      common::OB_SYS_TENANT_ID,
      CLUSTER_CURRENT_VERSION,
      tx_id,
      ls.get_ls_id(),
      common::ObAddr()))) {
    STORAGE_LOG(WARN, "failed to init mem_ctx for ut mds replay", K(ret), K(ls.get_ls_id()), K(tablet_id));
  } else {
    ctx.set_mem_ctx_for_replay(mem_ctx);
    if (OB_FAIL(ls.get_tablet_svr()->replay_set_tablet_status(tablet_id, replay_scn, user_data, ctx))) {
      if (OB_NO_NEED_UPDATE == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "failed to replay tablet status for test", K(ret), K(tablet_id), K(replay_scn));
      }
    } else if (OB_FAIL(ut_finish_memctx_mds_replay(mem_ctx, replay_scn))) {
      STORAGE_LOG(WARN, "failed to finish ut tablet status replay", K(ret), K(tablet_id), K(replay_scn));
    }
  }
  return ret;
}

inline int ut_replay_ddl_info_for_test(
    ObLS &ls,
    const common::ObTabletID &tablet_id,
    const ObTabletBindingMdsUserData &ddl_info,
    const share::SCN &commit_scn_hint,
    const transaction::ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  share::SCN replay_scn = commit_scn_hint;
  memtable::ObMemtableCtx mem_ctx;
  mds::MdsCtx ctx{mds::MdsWriter(tx_id)};
  if (OB_FAIL(ensure_mds_type_registry_for_ut())) {
    STORAGE_LOG(WARN, "failed to ensure mds type registry", K(ret));
  } else if (OB_FAIL(ensure_ls_mds_tablet_memtable_ready(ls))) {
    STORAGE_LOG(WARN, "failed to ensure ls mds tablet memtable", K(ret), K(ls.get_ls_id()));
  } else if (OB_FAIL(calc_test_tablet_mds_replay_scn(ls, commit_scn_hint, replay_scn))) {
    STORAGE_LOG(WARN, "failed to calc ut mds replay scn", K(ret), K(ls.get_ls_id()), K(tablet_id));
  } else if (OB_FAIL(mem_ctx.init(
      common::OB_SYS_TENANT_ID,
      CLUSTER_CURRENT_VERSION,
      tx_id,
      ls.get_ls_id(),
      common::ObAddr()))) {
    STORAGE_LOG(WARN, "failed to init mem_ctx for ut ddl replay", K(ret), K(ls.get_ls_id()), K(tablet_id));
  } else {
    ctx.set_mem_ctx_for_replay(mem_ctx);
    if (OB_FAIL(ls.get_tablet_svr()->replay_set_ddl_info(tablet_id, replay_scn, ddl_info, ctx))) {
      if (OB_NO_NEED_UPDATE == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "failed to replay ddl info for test", K(ret), K(tablet_id), K(replay_scn));
      }
    } else if (OB_FAIL(ut_finish_memctx_mds_replay(mem_ctx, replay_scn))) {
      STORAGE_LOG(WARN, "failed to finish ut ddl info replay", K(ret), K(tablet_id), K(replay_scn));
    }
  }
  return ret;
}

template <typename Key, typename Value>
inline int ut_replay_keyed_mds_for_test(
    ObLS &ls,
    const common::ObTabletID &tablet_id,
    const Key &key,
    const Value &value,
    const share::SCN &commit_scn_hint,
    const transaction::ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  share::SCN replay_scn = commit_scn_hint;
  ObTabletHandle tablet_handle;
  ObITabletMdsInterface *mds_if = nullptr;
  memtable::ObMemtableCtx mem_ctx;
  mds::MdsCtx ctx{mds::MdsWriter(tx_id)};
  if (OB_FAIL(ensure_mds_type_registry_for_ut())) {
    STORAGE_LOG(WARN, "failed to ensure mds type registry", K(ret));
  } else if (OB_FAIL(ensure_ls_mds_tablet_memtable_ready(ls))) {
    STORAGE_LOG(WARN, "failed to ensure ls mds tablet memtable", K(ret), K(ls.get_ls_id()));
  } else if (OB_FAIL(calc_test_tablet_mds_replay_scn(ls, commit_scn_hint, replay_scn))) {
    STORAGE_LOG(WARN, "failed to calc ut mds replay scn", K(ret), K(ls.get_ls_id()), K(tablet_id));
  } else if (OB_FAIL(ls.get_tablet_svr()->get_tablet(
      tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    STORAGE_LOG(WARN, "failed to get tablet for ut keyed mds replay", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tablet is null for ut keyed mds replay", K(ret), K(tablet_id));
  } else if (OB_FALSE_IT(mds_if = static_cast<ObITabletMdsInterface *>(tablet_handle.get_obj()))) {
  } else if (OB_FAIL(mem_ctx.init(
      common::OB_SYS_TENANT_ID,
      CLUSTER_CURRENT_VERSION,
      tx_id,
      ls.get_ls_id(),
      common::ObAddr()))) {
    STORAGE_LOG(WARN, "failed to init mem_ctx for ut keyed mds replay", K(ret), K(ls.get_ls_id()), K(tablet_id));
  } else {
    ctx.set_mem_ctx_for_replay(mem_ctx);
    if (OB_FAIL(mds_if->replay(key, value, ctx, replay_scn))) {
      if (OB_NO_NEED_UPDATE == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "failed to replay keyed mds for test", K(ret), K(tablet_id), K(replay_scn), K(key));
      }
    } else if (OB_FAIL(ut_finish_memctx_mds_replay(mem_ctx, replay_scn))) {
      STORAGE_LOG(WARN, "failed to finish ut keyed mds replay", K(ret), K(tablet_id), K(replay_scn));
    }
  }
  return ret;
}

template <typename T>
inline int ut_replay_dummy_key_mds_for_test(
    ObLS &ls,
    const common::ObTabletID &tablet_id,
    const T &data,
    const share::SCN &commit_scn_hint,
    const transaction::ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  share::SCN replay_scn = commit_scn_hint;
  ObTabletHandle tablet_handle;
  ObITabletMdsInterface *mds_if = nullptr;
  memtable::ObMemtableCtx mem_ctx;
  mds::MdsCtx ctx{mds::MdsWriter(tx_id)};
  if (OB_FAIL(ensure_mds_type_registry_for_ut())) {
    STORAGE_LOG(WARN, "failed to ensure mds type registry", K(ret));
  } else if (OB_FAIL(ensure_ls_mds_tablet_memtable_ready(ls))) {
    STORAGE_LOG(WARN, "failed to ensure ls mds tablet memtable", K(ret), K(ls.get_ls_id()));
  } else if (OB_FAIL(calc_test_tablet_mds_replay_scn(ls, commit_scn_hint, replay_scn))) {
    STORAGE_LOG(WARN, "failed to calc ut mds replay scn", K(ret), K(ls.get_ls_id()), K(tablet_id));
  } else if (OB_FAIL(ls.get_tablet_svr()->get_tablet(
      tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    STORAGE_LOG(WARN, "failed to get tablet for ut dummy key mds replay", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tablet is null for ut dummy key mds replay", K(ret), K(tablet_id));
  } else if (OB_FALSE_IT(mds_if = static_cast<ObITabletMdsInterface *>(tablet_handle.get_obj()))) {
  } else if (OB_FAIL(mem_ctx.init(
      common::OB_SYS_TENANT_ID,
      CLUSTER_CURRENT_VERSION,
      tx_id,
      ls.get_ls_id(),
      common::ObAddr()))) {
    STORAGE_LOG(WARN, "failed to init mem_ctx for ut dummy key mds replay", K(ret), K(ls.get_ls_id()), K(tablet_id));
  } else {
    ctx.set_mem_ctx_for_replay(mem_ctx);
    if (OB_FAIL(mds_if->replay(data, ctx, replay_scn))) {
      if (OB_NO_NEED_UPDATE == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "failed to replay dummy key mds for test", K(ret), K(tablet_id), K(replay_scn));
      }
    } else if (OB_FAIL(ut_finish_memctx_mds_replay(mem_ctx, replay_scn))) {
      STORAGE_LOG(WARN, "failed to finish ut dummy key mds replay", K(ret), K(tablet_id), K(replay_scn));
    }
  }
  return ret;
}

inline void build_test_transfer_out_tablet_status(
    const share::SCN &transfer_out_commit_scn,
    ObTabletCreateDeleteMdsUserData &user_data)
{
  user_data.reset();
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
  user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_OUT;
  user_data.transfer_scn_ = transfer_out_commit_scn;
  user_data.start_transfer_commit_scn_ = transfer_out_commit_scn;
  user_data.start_transfer_commit_version_ = transfer_out_commit_scn.get_val_for_tx();
}

// Write committed CREATE_TABLET status into LS aggregate MDS for READ_ALL_COMMITED readers.
// ObTabletStatus::MAX means caller will set tablet status via set_tablet_status later.
inline int replay_test_tablet_create_mds_status(
    ObLS &ls,
    ObLSTabletService &ls_tablet_svr,
    const common::ObTabletID &tablet_id,
    ObTablet &tablet,
    const ObTabletStatus::Status tablet_status,
    const share::SCN &create_commit_scn)
{
  int ret = OB_SUCCESS;
    if (ObTabletStatus::Status::MAX == tablet_status) {
    // Intentionally skip: mds scan / compat tests build MDS rows themselves.
  } else if (OB_FAIL(ensure_mds_type_registry_for_ut())) {
    STORAGE_LOG(WARN, "failed to ensure mds type registry", K(ret));
  } else {
    share::SCN replay_scn;
    ObTabletCreateDeleteMdsUserData user_data;
    memtable::ObMemtableCtx mem_ctx;
    const share::ObLSID ls_id = ls.get_ls_id();
    if (OB_FAIL(ensure_ls_mds_tablet_memtable_ready(ls))) {
      STORAGE_LOG(WARN, "failed to ensure ls mds tablet memtable", K(ret), K(ls_id));
    } else if (OB_FAIL(calc_test_tablet_mds_replay_scn(ls, create_commit_scn, replay_scn))) {
      STORAGE_LOG(WARN, "failed to calc mds replay scn", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(build_test_create_tablet_mds_user_data(
        tablet_status, create_commit_scn, replay_scn, user_data))) {
      STORAGE_LOG(WARN, "failed to build mds user data", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(mem_ctx.init(
        common::OB_SYS_TENANT_ID,
        CLUSTER_CURRENT_VERSION,
        transaction::ObTransID(tablet_id.id()),
        ls_id,
        common::ObAddr()))) {
      STORAGE_LOG(WARN, "failed to init mem_ctx for mds replay", K(ret), K(ls_id), K(tablet_id));
    } else {
      mds::MdsCtx ctx{mds::MdsWriter(mem_ctx.get_tx_id())};
      ctx.set_mem_ctx_for_replay(mem_ctx);
      if (OB_FAIL(ls_tablet_svr.replay_set_tablet_status(tablet_id, replay_scn, user_data, ctx))) {
        if (OB_NO_NEED_UPDATE == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "failed to replay tablet create mds status", K(ret), K(ls_id), K(tablet_id), K(replay_scn));
        }
      } else if (OB_FAIL(ut_finish_memctx_mds_replay(mem_ctx, replay_scn))) {
        STORAGE_LOG(WARN, "failed to commit tablet create mds replay", K(ret), K(ls_id), K(tablet_id), K(replay_scn));
      }
    }
    if (OB_SUCC(ret)) {
      tablet.tablet_status_cache_.set_value(user_data);
    }
  }
  return ret;
}

inline int TestTabletHelper::create_tablet(
    ObLSHandle &ls_handle,
    const common::ObTabletID &tablet_id,
    const share::schema::ObTableSchema &table_schema,
    common::ObArenaAllocator &allocator,
    const ObTabletStatus::Status tablet_status,
    const share::SCN &create_commit_scn,
    ObTabletHandle &handle,
    const StatusInjectMode inject_mode)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObLSTabletService *ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr();
  const lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
  ObArenaAllocator schema_allocator;
  ObCreateTabletSchema create_tablet_schema;

  ObTabletCreateSSTableParam param;
  prepare_sstable_param(tablet_id, table_schema, param);
  void *buff = nullptr;
  if (OB_FAIL(create_tablet_schema.init(schema_allocator, table_schema, compat_mode,
      false/*skip_column_info*/, DATA_VERSION_4_3_0_0))) {
    STORAGE_LOG(WARN, "failed to init storage schema", KR(ret), K(table_schema));
  } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_))) {
    STORAGE_LOG(WARN, "fill column checksum failed", K(ret), K(param));
  } else {
    const int64_t snapshot_version = 1;
    const share::ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
    ObFreezer *freezer = ls_handle.get_ls()->get_freezer();
    ObTabletHandle tablet_handle;
    const ObTabletMapKey key(ls_id, tablet_id);
    const bool need_create_empty_major_sstable =
      !(create_tablet_schema.is_user_hidden_table() || (create_tablet_schema.is_index_table() && !create_tablet_schema.can_read_index()));
    const bool need_generate_cs_replica_cg_array = 
      ls_handle.get_ls()->is_cs_replica() && create_tablet_schema.is_row_store() && create_tablet_schema.is_user_data_table();
    ObLS *ls = ls_handle.get_ls();
    if (OB_ISNULL(ls)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls is null", K(ret), K(ls_id));
    } else if (OB_FAIL(t3m->create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, tablet_handle))) {
      STORAGE_LOG(WARN, "t3m acquire tablet failed", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(tablet_handle.get_obj()->init_for_first_time_creation(
        *tablet_handle.get_allocator(),
        ls_id, tablet_id, tablet_id, share::SCN::base_scn(),
        snapshot_version, create_tablet_schema, need_create_empty_major_sstable, share::SCN::invalid_scn()/*clog_checkpoint_scn*/,
        share::ObSplitTabletInfo()/*split_info*/,
        share::ObForkTabletInfo()/*fork_info*/,
        false/*micro_index_clustered*/, need_generate_cs_replica_cg_array, false/*has_cs_replica*/, DATA_VERSION_4_3_0_0, freezer))) {
      STORAGE_LOG(WARN, "failed to init tablet", K(ret), K(ls_id), K(tablet_id));
    } else {
      ObUpdateTabletPointerParam param;
      if (OB_FAIL(tablet_handle.get_obj()->get_updating_tablet_pointer_param(param))) {
        STORAGE_LOG(WARN, "fail to get updating tablet pointer parameters", K(ret), K(tablet_handle));
      } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, tablet_handle, param))) {
        STORAGE_LOG(WARN, "failed to compare and swap tablet", K(ret), K(ls_id), K(tablet_id), K(param));
      } else if (OB_FAIL(ls_tablet_svr->tablet_id_set_.set(tablet_id))) {
        STORAGE_LOG(WARN, "set tablet id failed", K(ret), K(tablet_id));
      } else if (OB_FAIL(replay_test_tablet_create_mds_status(
          *ls, *ls_tablet_svr, tablet_id, *tablet_handle.get_obj(), tablet_status, create_commit_scn))) {
        STORAGE_LOG(WARN, "failed to write test tablet mds status", K(ret), K(ls_id), K(tablet_id),
            K(tablet_status), K(create_commit_scn));
      } else if (StatusInjectMode::MIGRATED == inject_mode
          && OB_FAIL(tablet_handle.get_obj()->reset_tablet_status_written())) {
        // Simulate a tablet brought in by migration: committed status is in
        // LS aggregate MDS, but this server never wrote it, so the in-memory
        // tablet_mds_status_written_ flag must be false.
        STORAGE_LOG(WARN, "failed to reset tablet_status_written for migrated-mode helper",
            K(ret), K(ls_id), K(tablet_id));
      } else {
        handle.assign(tablet_handle);
      }
    }
  }
  return ret;
}

inline int TestTabletHelper::create_tablet(
    ObLSHandle &ls_handle,
    const common::ObTabletID &tablet_id,
    const share::schema::ObTableSchema &table_schema,
    common::ObArenaAllocator &allocator,
    const ObTabletStatus::Status tablet_status,
    const share::SCN &create_commit_scn,
    const StatusInjectMode inject_mode)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;

  if (OB_FAIL(create_tablet(ls_handle, tablet_id, table_schema, allocator, tablet_status, create_commit_scn, tablet_handle, inject_mode))) {
    STORAGE_LOG(WARN, "failed to create tablet", K(ret), K(tablet_id));
  }

  return ret;
}

inline int TestTabletHelper::remove_tablet(const ObLSHandle &ls_handle, const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  ObTabletHandle tablet_handle;
  ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle);
  const share::ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObMetaDiskAddr disk_addr;
  ObUpdateTabletPointerParam param;
  disk_addr.set_mem_addr(0, sizeof(ObTablet));
  if (OB_FAIL(tablet_handle.get_obj()->get_updating_tablet_pointer_param(param))) {
    STORAGE_LOG(WARN, "fail to get updating tablet pointer parameters", K(ret), K(tablet_handle));
  } else if(OB_FAIL(t3m->compare_and_swap_tablet(
          ObTabletMapKey(ls_id, tablet_id), tablet_handle, tablet_handle, param))) {
    STORAGE_LOG(WARN, "failed to compare and swap tablet", K(ret), K(ls_id), K(tablet_id), K(disk_addr));
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TEST_TABLET_HELPER
