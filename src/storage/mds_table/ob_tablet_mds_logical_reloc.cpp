/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX MDS

#include "storage/mds_table/ob_tablet_mds_logical_reloc.h"
#include "storage/mds_table/ob_tablet_mds_reloc_batch_arg.h"
#include "storage/mds_table/ob_tablet_mds_reloc_traits.h"
#include "storage/mds_table/ob_mds_table.h"
#include "storage/mds_table/ob_mds_type_id_map.h"
#include "storage/mds_table/ob_mds_multikey_reader.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"
#include "storage/tablet/ob_tablet_split_mds_user_data.h"
#include "storage/tablet/ob_tablet_split_info_mds_user_data.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_data.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "storage/high_availability/ob_transfer_struct.h"
#include "storage/tx/ob_tx_log.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_cluster_version.h"
#include "share/transfer/ob_transfer_task_operator.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "lib/utility/serialization.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace storage
{

using namespace share;
using namespace common;

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_MDS_RELOC_END_TRANS_RESULT);
#endif

// Minimum data version at which MDS relocation uses op-semantics (V2): selected
// multi-key rows are no longer materialized into the redo; each replayer pulls
// them from the local source LS replica so the dest row-set stays consistent
// with that replica's source view (fixes the backup/restore gap).
//
// Transfer, split, and fork share this helper. Each caller supplies its own
// snapshot, destination tablet list, and reloc type. All three pin the
// task data version so retries cannot switch payload format.
//
// V2 is enabled only in shared-storage mode.  Its replay can pull op rows from
// the local source tablet after restart, so source invalidation relies on the
// shared-storage replace/refresh checkpoint fences. This repository supports only
// shared storage; the retained non-SS version branch is legacy compatibility,
// not a supported transfer execution path in this repository.
//
// V2 is assigned to data version 4.6.3.1. STANDBY_UPGRADE_DATA_VERSION orders
// replay on standby; below 4.6.3.1 writers retain V1, including DDL tasks
// pinned to an older data-format version.
namespace
{
class ObMdsRelocUtil final
{
public:
  static bool is_reloc_v2_enabled(const uint64_t data_version);

  // ======================== Param ========================

  struct Param final
  {
    Param()
      : dest_tablet_ids_(nullptr),
        read_snapshot_(),
        reloc_type_(ObTabletMdsRelocType::INVALID),
        dest_transfer_scn_(),
        tenant_id_(OB_INVALID_ID),
        dest_ls_id_(),
        src_ls_id_(),
        src_tablet_id_(),
        src_reorg_scn_(),
        is_reloc_v2_enabled_(false)
    {}

    bool is_valid() const
    {
      bool is_param_valid = nullptr != dest_tablet_ids_
          && dest_tablet_ids_->count() > 0
          && read_snapshot_.is_valid()
          && is_valid_reloc_type(reloc_type_)
          && dest_transfer_scn_.is_valid()
          && OB_INVALID_ID != tenant_id_
          && dest_ls_id_.is_valid();
      // V2 pulls op-type rows on replay from the source LS replica, which requires
      // the source key (src_ls_id_ / src_tablet_id_).
      // src_reorg_scn_ is carried into the target and checked against the
      // source tablet generation before V2 replay pull reads op rows.
      if (is_param_valid && is_reloc_v2_enabled_) {
        is_param_valid = src_ls_id_.is_valid() && src_tablet_id_.is_valid() && src_reorg_scn_.is_valid();
      }
      return is_param_valid;
    }

    const ObIArray<ObTabletID> *dest_tablet_ids_;
    SCN read_snapshot_;
    ObTabletMdsRelocType reloc_type_;
    SCN dest_transfer_scn_;
    uint64_t tenant_id_;
    ObLSID dest_ls_id_;
    // V2 op-semantic fields.  src_reorg_scn_ is the source tablet generation
    // snapshot carried into batch args and checked during replay pull before
    // reading op rows.
    ObLSID src_ls_id_;
    ObTabletID src_tablet_id_;  // source tablet (== dest tablet for transfer)
    SCN src_reorg_scn_;
    bool is_reloc_v2_enabled_;
  };

  static int submit_tablet_logical_reloc_in_trans(
      ObTablet &source_tablet,
      const Param &param);

private:
  static int build_batch_(const Param &param, ObTabletMdsRelocBatchArg &batch);
  static int submit_selected_rows_(
      ObTablet &source_tablet, ObMdsTable &mds_table, const Param &param,
      ObArenaAllocator &allocator, observer::ObInnerSQLConnection &conn,
      ObTabletMdsRelocBatchArg &batch, bool &final_batch_registered);
  static int submit_final_batch_(
      const bool need_final_batch, const Param &param,
      ObArenaAllocator &allocator, observer::ObInnerSQLConnection &conn,
      ObTabletMdsRelocBatchArg &batch, bool &final_batch_registered);
  static int register_reloc_batch_via_inner_sql_conn_(
      observer::ObInnerSQLConnection &conn,
      const uint64_t tenant_id,
      ObArenaAllocator &allocator,
      ObTabletMdsRelocBatchArg &batch,
      bool &final_batch_registered);
  static int flush_before_push_if_needed_(
      ObTabletMdsRelocBatchArg &batch,
      ObArenaAllocator &allocator,
      const uint64_t tenant_id,
      observer::ObInnerSQLConnection &conn,
      const int64_t incoming_row_serialize_size,
      bool &final_batch_registered);
  template <typename K, typename V>
  static int serialize_reloc_row_payload_(
      const K &key,
      const V &value,
      const int64_t payload_size,
      ObArenaAllocator &allocator,
      ObString &payload);
  static bool need_filter_row_(
      const Param &param,
      const ObMdsValueMeta &mds_meta);
  template <typename K, typename V>
  static int submit_reloc_rows_by_type_(
      ObTablet &source_tablet,
      ObMdsTable &mds_table,
      const Param &param,
      ObArenaAllocator &allocator,
      observer::ObInnerSQLConnection &conn,
      ObTabletMdsRelocBatchArg &batch,
      bool &final_batch_registered);
};

// Uniform interface: init(tablet, mds_table, snapshot) → get_next(alloc, k, v, meta).
// There is no explicit release: key/value are deep-deserialized from the caller-provided
// allocator, so the caller must let them go out of scope (i.e. destroy them) before that
// allocator is reused.
// DummyKey reader: init records tablet, snapshot and MDS table; get_next point-gets one row then ITER_END.
// Multi-key reader: wraps scan reader.
template <typename K, typename V, typename Enable = void> struct ObMdsRelocReader;

// DummyKey reader: point-get in get_next, yields one row then ITER_END.
template <typename V>
struct ObMdsRelocReader<mds::DummyKey, V>
{
  ObMdsRelocReader() : mds_table_(nullptr), tablet_id_(), src_reorg_scn_(), snapshot_(), done_(false) {}
  int init(ObTablet &tablet, ObMdsTable &mds_table, const SCN &snapshot)
  {
    tablet_id_ = tablet.get_tablet_meta().tablet_id_;
    src_reorg_scn_ = tablet.get_reorganization_scn();
    snapshot_ = snapshot;
    mds_table_ = &mds_table;
    done_ = false;
    return OB_SUCCESS;
  }
  int get_next(ObArenaAllocator &allocator, mds::DummyKey &, V &v, ObMdsValueMeta &m)
  {
    int ret = OB_SUCCESS;
    const int64_t timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US;
    if (done_) {
      ret = OB_ITER_END;
    } else if (FALSE_IT(done_ = true)) {
    } else if (OB_FAIL(mds_table_->get<V>(&allocator, tablet_id_, src_reorg_scn_,
                                          snapshot_, timeout_us, false/*read_uncommitted*/, v, m))) {
      if (OB_ITER_END == ret || OB_EMPTY_RESULT == ret) {
        ret = OB_ITER_END;
      } else {
        constexpr uint8_t mds_type_id = static_cast<uint8_t>(mds_type_id_detail::TypeIdOf<mds::DummyKey, V>::value);
        LOG_WARN("point-get dummy-key mds failed", KR(ret), K(tablet_id_), K(mds_type_id),
                 "value_type", typeid(V).name());
      }
    }
    return ret;
  }
private:
  ObMdsTable *mds_table_;
  ObTabletID tablet_id_;
  SCN src_reorg_scn_;
  SCN snapshot_;
  bool done_;
};

// Multi-key reader: wraps ObTabletMDSInfoReader<K, V> for all multi-key types.
template <typename K, typename V>
struct ObMdsRelocReader<K, V, typename std::enable_if<!std::is_same<K, mds::DummyKey>::value>::type>
{
  int init(ObTablet &tablet, ObMdsTable &, const SCN &snapshot) { return reader_.init(tablet, snapshot); }
  int get_next(ObArenaAllocator &alloc, K &k, V &v, ObMdsValueMeta &m)
  { return reader_.get_next_mds_kv(alloc, k, v, m); }
private:
  ObTabletMDSInfoReader<K, V> reader_;
};
} // anonymous namespace

// ======================== Helpers ========================


int ObMdsRelocUtil::register_reloc_batch_via_inner_sql_conn_(
    observer::ObInnerSQLConnection &conn,
    const uint64_t tenant_id,
    ObArenaAllocator &allocator,
    ObTabletMdsRelocBatchArg &batch,
    bool &final_batch_registered)
{
  int ret = OB_SUCCESS;
  const bool registering_final = batch.is_final_;
  transaction::ObRegisterMdsFlag flag;
  flag.need_flush_redo_instantly_ = true;
  if (OB_UNLIKELY(final_batch_registered)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("must not register reloc batch after final batch", KR(ret), K(batch));
    OB_ASSERT(!final_batch_registered);
  } else if (OB_UNLIKELY(!batch.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid reloc batch", KR(ret), K(batch));
  } else if (batch.rows_.count() > 0
             && OB_FAIL(batch.skip_bitmap_.init(batch.rows_.count(), batch.targets_.count()))) {
    LOG_WARN("init skip bitmap failed", KR(ret));
  } else {
    const int64_t buf_len = batch.get_serialize_size();
    int64_t pos = 0;
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc buf failed", KR(ret), K(buf_len));
    } else if (OB_FAIL(batch.serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize batch failed", KR(ret));
    } else if (OB_UNLIKELY(pos != buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("serialize len mismatch", KR(ret), K(pos), K(buf_len));
    } else if (OB_UNLIKELY(!transaction::ObTxMultiDataSourceLog::can_hold_node_data(
                   transaction::ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH, buf_len))) {
      ret = OB_LOG_TOO_LARGE;
      LOG_WARN("reloc batch exceeds mds log size limit", KR(ret), K(buf_len), K(batch));
    } else if (OB_FAIL(conn.register_multi_data_source(tenant_id,
                           batch.ls_id_,
                           transaction::ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH,
                           buf,
                           buf_len,
                           flag))) {
      if (OB_ALREADY_DONE != ret) {
        LOG_WARN("register reloc batch failed", KR(ret), K(tenant_id), K(batch.ls_id_));
      }
    } else {
      batch.reset_rows();
      allocator.reuse();
      final_batch_registered = registering_final;
    }
  }
  return ret;
}

// Flush the current batch before pushing a row that would make the batch
// un-registrable. The bound is the transaction layer MDS node capacity, not
// MAX_MDS_LOG_SIZE: a batch that is merely smaller than MAX_MDS_LOG_SIZE
// registers successfully but can never be submitted (Dima 2026092400119415025).
int ObMdsRelocUtil::flush_before_push_if_needed_(
    ObTabletMdsRelocBatchArg &batch,
    ObArenaAllocator &allocator,
    const uint64_t tenant_id,
    observer::ObInnerSQLConnection &conn,
    const int64_t incoming_row_serialize_size,
    bool &final_batch_registered)
{
  int ret = OB_SUCCESS;
  const int64_t after_push_size =
      batch.estimate_serialize_size_after_push_row(incoming_row_serialize_size);
  if (OB_UNLIKELY(!transaction::ObTxMultiDataSourceLog::can_hold_node_data(
          transaction::ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH, after_push_size))) {
    if (batch.rows_.count() > 0) {
      ret = register_reloc_batch_via_inner_sql_conn_(conn, tenant_id, allocator, batch,
                                                    final_batch_registered);
      if (OB_FAIL(ret)) {
        LOG_WARN("flush batch before push failed", KR(ret), K(after_push_size));
      }
    } else {
      // The row does not fit into an empty batch, so it can never be registered.
      ret = OB_LOG_TOO_LARGE;
      LOG_WARN("single reloc row exceeds mds log size limit", KR(ret), K(after_push_size));
    }
  }
  return ret;
}

// ======================== Reloc row serialization ========================

// Payload format: serialized K + serialized V.
// For DummyKey, K serializes to 0 bytes.
template <typename K, typename V>
int ObMdsRelocUtil::serialize_reloc_row_payload_(
    const K &key,
    const V &value,
    const int64_t payload_size,
    ObArenaAllocator &allocator,
    ObString &payload)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(payload_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc reloc payload failed", KR(ret), K(payload_size));
  } else if (OB_FAIL(key.mds_serialize(buf, payload_size, pos))) {
    LOG_WARN("mds_serialize key failed", KR(ret));
  } else if (OB_FAIL(value.serialize(buf, payload_size, pos))) {
    LOG_WARN("serialize value failed", KR(ret));
  } else if (OB_UNLIKELY(pos != payload_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reloc payload serialize len mismatch", KR(ret), K(pos), K(payload_size));
  } else {
    payload.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

// ======================== Unified submit_reloc_rows_by_type_ ========================

// All relocatable MDS rows are read at the latest physical snapshot.
// A finite read_snapshot_ is the business snapshot and filters rows committed later.
bool ObMdsRelocUtil::need_filter_row_(
    const Param &param,
    const ObMdsValueMeta &mds_meta)
{
  return !param.read_snapshot_.is_max()
      && mds_meta.commit_version_ > param.read_snapshot_.get_val_for_tx();
}

template <typename K, typename V>
int ObMdsRelocUtil::submit_reloc_rows_by_type_(
    ObTablet &source_tablet,
    ObMdsTable &mds_table,
    const Param &param,
    ObArenaAllocator &allocator,
    observer::ObInnerSQLConnection &conn,
    ObTabletMdsRelocBatchArg &batch,
    bool &final_batch_registered)
{
  int ret = OB_SUCCESS;
  constexpr uint8_t mds_type_id = static_cast<uint8_t>(mds_type_id_detail::TypeIdOf<K, V>::value);
  // V2: selected multi-key units are not materialized into the redo; each
  // replayer pulls them from the local source LS replica.  Skip shipping.
  if (!(param.is_reloc_v2_enabled_ && ObIsRelocOpType<K>::value)) {
    ObMdsRelocReader<K, V> reader;
    ObArenaAllocator read_allocator(ObMemAttr(MTL_ID(), "RelocRead"));
    if (OB_FAIL(reader.init(source_tablet, mds_table, SCN::max_scn()))) {
      LOG_WARN("reloc reader init failed", KR(ret), K(mds_type_id),
               "key_type", typeid(K).name(), "value_type", typeid(V).name());
    }
    while (OB_SUCC(ret)) {
      {
        K key;
        V value;
        ObMdsValueMeta mds_meta;
        ObString payload;
        int64_t row_serialize_size = 0;
        int64_t payload_size = 0;
        ObTabletMdsRelocRowStorage row;
        bool need_filter = false;
        ret = reader.get_next(read_allocator, key, value, mds_meta);
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("reloc get_next failed", KR(ret), K(mds_type_id),
                   "key_type", typeid(K).name(), "value_type", typeid(V).name());
        } else if (need_filter_row_(param, mds_meta)) {
          need_filter = true;
          // Rows committed after read_snapshot_ do not belong to this relocation snapshot.
        } else {
          row.mds_unit_id_ = mds_type_id;
          row.meta_ = mds_meta;
          const int64_t ksz = key.mds_get_serialize_size();
          const int64_t vsz = value.get_serialize_size();
          payload_size = ksz + vsz;
          if (OB_UNLIKELY(payload_size > INT32_MAX)) {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("reloc payload serialize size exceeds ObString limit", KR(ret), K(mds_type_id), K(payload_size),
                     "key_type", typeid(K).name(), "value_type", typeid(V).name());
          } else {
            row_serialize_size = row.get_serialize_size()
                + serialization::encoded_length_vstr(payload_size)
                - serialization::encoded_length_vstr(int64_t(0));
          }
        }
        if (OB_FAIL(ret) || need_filter) {
        } else if (OB_FAIL(flush_before_push_if_needed_(
                batch, allocator, param.tenant_id_, conn, row_serialize_size,
                final_batch_registered))) {
          LOG_WARN("flush reloc batch before push failed", KR(ret), K(mds_type_id), K(row_serialize_size),
                   "key_type", typeid(K).name(), "value_type", typeid(V).name());
        } else if (OB_FAIL(serialize_reloc_row_payload_(key, value, payload_size, allocator, payload))) {
          LOG_WARN("serialize reloc payload failed", KR(ret), K(mds_type_id),
                   "key_type", typeid(K).name(), "value_type", typeid(V).name());
        } else {
          row.ser_payload_ = payload;
          if (OB_FAIL(batch.push_row(row))) {
            LOG_WARN("push reloc row failed", KR(ret), K(mds_type_id),
                     "key_type", typeid(K).name(), "value_type", typeid(V).name());
          }
        }
      } // destroy key/value before reusing read_allocator
      read_allocator.reuse();
    }
  }
  return ret;
}

int ObMdsRelocUtil::submit_final_batch_(
    const bool need_final_batch, const Param &param,
    ObArenaAllocator &allocator, observer::ObInnerSQLConnection &conn,
    ObTabletMdsRelocBatchArg &batch, bool &final_batch_registered)
{
  int ret = OB_SUCCESS;
  // Transfer and fork always end with one final batch.  It may be empty when
  // the source tablet has no relocatable MDS rows.  For V1 this batch inserts
  // the completion marker.  For V2 it updates the marker inserted by the lock
  // batch.  Keeping that update as the LAST registration gives the marker
  // a redo SCN no earlier than any payload or pulled-op redo.  The persisted
  // gate can therefore compare this final marker redo SCN with the SS
  // MDS-tablet clog checkpoint SCN; commit SCN is deliberately not used because
  // it is not an MDS-memtable redo SCN and is normally greater than the redo it
  // commits.
  if (need_final_batch) {
    batch.is_final_ = true;
    OB_ASSERT(!final_batch_registered);
    if (OB_FAIL(register_reloc_batch_via_inner_sql_conn_(
            conn, param.tenant_id_, allocator, batch, final_batch_registered))) {
      if (OB_ALREADY_DONE != ret) {
        LOG_WARN("register reloc final batch failed", KR(ret), K(param.reloc_type_));
      }
    }
  } else if (batch.rows_.count() > 0) {
    // Only V1 split omits the final marker.
    if (OB_FAIL(register_reloc_batch_via_inner_sql_conn_(
            conn, param.tenant_id_, allocator, batch, final_batch_registered))) {
      LOG_WARN("flush remaining batch failed", KR(ret));
    }
  }
  return ret;
}

// ======================== Unified entry ========================

int ObMdsRelocUtil::submit_tablet_logical_reloc_in_trans(
    ObTablet &source_tablet,
    const Param &param)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  observer::ObInnerSQLConnection *conn = nullptr;
  ObLSHandle ls_handle;
  ObMdsTable *mds_table = nullptr;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "RelocBatch"));
  ObTabletMdsRelocBatchArg batch;
  bool final_batch_registered = false;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical reloc param", KR(ret), K(param.tenant_id_), K(param.dest_ls_id_),
             K(param.read_snapshot_), K(param.reloc_type_), K(param.dest_transfer_scn_),
             K(param.src_ls_id_), K(param.src_tablet_id_), K(param.src_reorg_scn_),
             "is_reloc_v2_enabled", param.is_reloc_v2_enabled_);
  } else if (OB_UNLIKELY(nullptr == GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy null", KR(ret));
  } else if (OB_FAIL(source_tablet.get_mds_table(ls_handle, mds_table))) {
    LOG_WARN("get src mds table failed", KR(ret));
  } else if (OB_ISNULL(mds_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src mds table null", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, param.tenant_id_, false /*with_snapshot*/, share::OBCG_STORAGE))) {
    LOG_WARN("start trans failed", KR(ret), K(param.tenant_id_));
  } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner conn null", KR(ret));
  } else if (OB_FAIL(build_batch_(param, batch))) {
    LOG_WARN("build reloc batch failed", KR(ret));
  } else if (batch.is_v2()) {
    // Acquire the per-(tablet, destination-generation) mutex before registering
    // any payload or op-semantic row.  The empty non-final V2 batch inserts the
    // reloc marker with check_dup=true in on_register(), so a concurrent reloc
    // conflicts here and aborts atomically without first modifying destination
    // MDS rows.  This marker remains uncommitted until the surrounding SQL
    // transaction commits; readers therefore cannot mistake it for completion.
    if (OB_FAIL(register_reloc_batch_via_inner_sql_conn_(
            *conn, param.tenant_id_, allocator, batch, final_batch_registered))) {
      if (OB_ALREADY_DONE != ret) {
        LOG_WARN("register reloc marker lock batch failed", KR(ret), K(param.reloc_type_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(submit_selected_rows_(
        source_tablet, *mds_table, param, allocator, *conn, batch, final_batch_registered))) {
      LOG_WARN("submit selected reloc rows failed", KR(ret));
    }
  }

  // V2 split also ends with a final batch so the op-type pull is triggered once
  // (V1 split published no marker).
  const bool need_final_batch =
      ObTabletMdsRelocType::TRANSFER == param.reloc_type_
      || ObTabletMdsRelocType::FORK == param.reloc_type_
      || (ObTabletMdsRelocType::SPLIT == param.reloc_type_ && param.is_reloc_v2_enabled_);
  if (OB_SUCC(ret)) {
    ret = submit_final_batch_(
        need_final_batch, param, allocator, *conn, batch, final_batch_registered);
  }

  // End transaction.
  if (OB_SUCC(ret)) {
    // No multi-data-source registration may be added between this assertion
    // and trans.end(): final must remain the last registration of the txn.
    OB_ASSERT(!need_final_batch || final_batch_registered);
  }
  const bool reloc_already_done = OB_ALREADY_DONE == ret;
  if (trans.is_started()) {
    int tmp_ret = trans.end(OB_SUCC(ret));
#ifdef ERRSIM
    // Exercise end-result propagation after the real commit/abort has cleaned
    // up the transaction; never leave a live transaction behind for the test.
    if (OB_SUCCESS == tmp_ret) {
      tmp_ret = OB_E(EN_MDS_RELOC_END_TRANS_RESULT, GETTID()) OB_SUCCESS;
    }
#endif
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end trans failed", KR(tmp_ret), KR(ret));
      if (reloc_already_done || OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    } else if (reloc_already_done) {
      // The committed marker belongs to the winning reloc transaction.  This
      // attempt has now been aborted successfully, so report idempotent success
      // to the task instead of scheduling another retry.
      ret = OB_SUCCESS;
    }
  }
  // Record the submit decision (v1/v2) into the driving operation's event module
  // (worker-thread context, safe).  Per-submit granularity matches the existing
  // per-tablet transfer events.
  {
    const int64_t tablet_cnt = (nullptr != param.dest_tablet_ids_) ? param.dest_tablet_ids_->count() : 0;
    SERVER_EVENT_ADD(reloc_event_module(param.reloc_type_), "MDS_RELOC_SUBMIT",
        "already_done", reloc_already_done,
        "is_v2", param.is_reloc_v2_enabled_,
        "src_ls_id", param.src_ls_id_.id(),
        "dest_ls_id", param.dest_ls_id_.id(),
        "tablet_cnt", tablet_cnt,
        "result", ret);
  }
  return ret;
}

int ObMdsRelocUtil::build_batch_(const Param &param, ObTabletMdsRelocBatchArg &batch)
{
  int ret = OB_SUCCESS;
  // Build targets.
  batch.reloc_type_ = param.reloc_type_;
  batch.ls_id_ = param.dest_ls_id_;
  batch.version_ = param.is_reloc_v2_enabled_
      ? LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION_V2
      : LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION;
  if (param.is_reloc_v2_enabled_) {
    batch.src_ls_id_ = param.src_ls_id_;
    batch.read_snapshot_ = param.read_snapshot_;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param.dest_tablet_ids_->count(); ++i) {
    ObTabletMdsRelocTarget target;
    target.tablet_id_ = param.dest_tablet_ids_->at(i);
    target.transfer_scn_ = param.dest_transfer_scn_;
    if (param.is_reloc_v2_enabled_) {
      // All dest targets share one source tablet (== dest tablet for transfer,
      // the split/fork source otherwise).
      target.src_tablet_id_ = param.src_tablet_id_;
      target.src_reorg_scn_ = param.src_reorg_scn_;
    }
    if (OB_FAIL(batch.targets_.push_back(target))) {
      LOG_WARN("push reloc target failed", KR(ret));
    }
  }
  return ret;
}

int ObMdsRelocUtil::submit_selected_rows_(
    ObTablet &source_tablet, ObMdsTable &mds_table, const Param &param,
    ObArenaAllocator &allocator, observer::ObInnerSQLConnection &conn,
    ObTabletMdsRelocBatchArg &batch, bool &final_batch_registered)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && ObTabletMdsRelocType::FORK == param.reloc_type_
      && OB_FAIL((submit_reloc_rows_by_type_<ObTTLFilterInfoKey, ObTTLFilterInfo>(
          source_tablet, mds_table, param, allocator, conn, batch, final_batch_registered)))) {
    LOG_WARN("submit fork ttl reloc rows failed", KR(ret));
  }
  // Expand all types from mds_register.h.
#define GENERATE_LS_INNER_MDS_TABLE
#define _GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION, TRANSFER_RELOC, SPLIT_RELOC) \
  if (OB_SUCC(ret)) { \
    if constexpr ((TRANSFER_RELOC) || (SPLIT_RELOC)) { \
      if ((ObTabletMdsRelocType::TRANSFER == param.reloc_type_ && (TRANSFER_RELOC)) \
          || (ObTabletMdsRelocType::SPLIT == param.reloc_type_ && (SPLIT_RELOC))) { \
        if (OB_FAIL((submit_reloc_rows_by_type_<KEY_TYPE, VALUE_TYPE>( \
                source_tablet, mds_table, param, allocator, conn, batch, \
                final_batch_registered)))) { \
          LOG_WARN("submit reloc rows failed", KR(ret), \
                   "key_type", #KEY_TYPE, "value_type", #VALUE_TYPE); \
        } \
      } \
    } \
  }
#include "storage/multi_data_source/compile_utility/mds_register.h"
#undef _GENERATE_MDS_UNIT_
#undef GENERATE_LS_INNER_MDS_TABLE

  return ret;
}

// ======================== Public API ========================

int ObTabletTransferMdsReloc::get_task_data_version_(
    const ObTransferTask &task,
    const ObLSID &src_ls_id,
    const ObLSID &dest_ls_id,
    const SCN &transfer_start_scn,
    uint64_t &data_version)
{
  int ret = OB_SUCCESS;
  data_version = 0;
  if (task.get_src_ls() != src_ls_id || task.get_dest_ls() != dest_ls_id
      || task.get_start_scn() != transfer_start_scn) {
    // A stale backfill must never borrow a replacement task's protocol.
    ret = OB_EAGAIN;
    LOG_WARN("transfer task generation changed before mds relocation",
        K(ret), K(task), K(src_ls_id), K(dest_ls_id), K(transfer_start_scn));
  } else if (0 == task.get_data_version()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer task has no data version", K(ret), K(task));
  } else {
    data_version = task.get_data_version();
  }
  return ret;
}

int ObTabletTransferMdsReloc::submit_transfer_mds_reloc(
    ObTablet &src_tablet,
    const SCN &dest_transfer_start_scn,
    const uint64_t tenant_id,
    const ObLSID &dest_ls_id)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  ObTransferTask task;
  uint64_t data_version = 0;
  const ObLSID src_ls_id = src_tablet.get_tablet_meta().ls_id_;
  ObSEArray<ObTabletID, 1> dest_tablet_ids;
  const ObTabletID tablet_id = src_tablet.get_tablet_meta().tablet_id_;
  if (OB_UNLIKELY(!dest_transfer_start_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dest_transfer_start_scn", KR(ret), K(dest_transfer_start_scn));
  } else if (OB_FAIL(dest_tablet_ids.push_back(tablet_id))) {
    LOG_WARN("push back dest tablet id failed", KR(ret), K(tablet_id));
  } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(false /*get_commit*/, &src_tablet, user_data))) {
    LOG_WARN("get tablet status failed", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null when reading transfer task version", K(ret));
  } else if (OB_FAIL(ObTransferTaskOperator::get_by_src_ls(
      *GCTX.sql_proxy_, tenant_id, src_ls_id, task, share::OBCG_STORAGE))) {
    LOG_WARN("failed to read transfer task before mds relocation", K(ret), K(src_ls_id));
  } else if (OB_FAIL(get_task_data_version_(
      task, src_ls_id, dest_ls_id, dest_transfer_start_scn, data_version))) {
    LOG_WARN("failed to get this transfer generation's data version", K(ret), K(task));
  } else {
    const SCN read_snapshot = share::SCN::max_scn();
    const SCN src_reorg_scn = src_tablet.get_reorganization_scn();
    ObMdsRelocUtil::Param param;
    param.dest_tablet_ids_ = &dest_tablet_ids;
    param.read_snapshot_ = read_snapshot;
    param.reloc_type_ = ObTabletMdsRelocType::TRANSFER;
    param.dest_transfer_scn_ = dest_transfer_start_scn;
    param.tenant_id_ = tenant_id;
    param.dest_ls_id_ = dest_ls_id;
    param.src_ls_id_ = src_ls_id;
    param.src_tablet_id_ = tablet_id;
    param.src_reorg_scn_ = src_reorg_scn;
    param.is_reloc_v2_enabled_ = ObMdsRelocUtil::is_reloc_v2_enabled(data_version);
    if (OB_FAIL(ObMdsRelocUtil::submit_tablet_logical_reloc_in_trans(src_tablet, param))) {
      LOG_WARN("submit transfer logical reloc failed", KR(ret), K(tablet_id), K(dest_ls_id),
               K(dest_transfer_start_scn), K(read_snapshot), K(src_ls_id), K(src_reorg_scn),
               K(data_version), "is_reloc_v2_enabled", param.is_reloc_v2_enabled_);
    }
  }
  return ret;
}

void ObTabletSplitMdsReloc::try_flush_mds_table_(
    ObLS *ls,
    const share::SCN &snapshot_scn,
    const ObIArray<ObTabletID> &tablet_ids)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(ls)) {
    tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN_RET(tmp_ret, "split reloc flush requires ls",
        KR(tmp_ret), K(snapshot_scn), K(tablet_ids));
  } else if (OB_TMP_FAIL(ls->flush_mds_table())) {
    LOG_WARN_RET(tmp_ret, "failed to trigger mds table flush after split reloc submit",
        KR(tmp_ret), K(ls->get_ls_id()), K(snapshot_scn), K(tablet_ids));
  }
}

int ObTabletSplitMdsReloc::submit_split_multikey_mds_reloc(
    ObTablet &source_tablet,
    const ObIArray<ObTabletID> &dest_tablet_ids,
    const SCN &snapshot_scn,
    const uint64_t tenant_id,
    const ObLSID &dest_ls_id,
    const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!snapshot_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid snapshot scn for split reloc", KR(ret), K(snapshot_scn));
  } else if (OB_UNLIKELY(0 >= dest_tablet_ids.count() || !dest_ls_id.is_valid()
      || OB_INVALID_ID == tenant_id || 0 == data_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid split reloc args", KR(ret), K(dest_tablet_ids.count()),
        K(tenant_id), K(data_version), K(dest_ls_id));
  } else {
    ObMdsRelocUtil::Param param;
    param.dest_tablet_ids_ = &dest_tablet_ids;
    param.read_snapshot_ = snapshot_scn;
    param.reloc_type_ = ObTabletMdsRelocType::SPLIT;
    param.dest_transfer_scn_ = SCN::min_scn();
    param.tenant_id_ = tenant_id;
    param.dest_ls_id_ = dest_ls_id;
    param.src_ls_id_ = source_tablet.get_tablet_meta().ls_id_;
    param.src_tablet_id_ = source_tablet.get_tablet_meta().tablet_id_;
    param.src_reorg_scn_ = source_tablet.get_reorganization_scn();
    // Split execution is pinned to the DDL task's data-format version.  Using
    // the live tenant version here could switch an in-flight V1 split to V2
    // during upgrade while its completion gate still follows the pinned task
    // version, allowing source invalidation before the V2 redo is persisted.
    param.is_reloc_v2_enabled_ = ObMdsRelocUtil::is_reloc_v2_enabled(data_version);
    if (OB_FAIL(ObMdsRelocUtil::submit_tablet_logical_reloc_in_trans(source_tablet, param))) {
      LOG_WARN("submit split logical reloc failed", KR(ret), K(dest_ls_id), K(snapshot_scn),
               K(data_version), "is_reloc_v2_enabled", param.is_reloc_v2_enabled_);
    }
  }
  return ret;
}

int ObTabletForkMdsReloc::submit_fork_ttl_mds_reloc(
    ObTablet &source_tablet,
    const ObTabletID &dest_tablet_id,
    const SCN &snapshot_scn,
    const SCN &dest_reorg_scn,
    const uint64_t tenant_id,
    const ObLSID &dest_ls_id,
    const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 1> dest_tablet_ids;
  if (OB_UNLIKELY(!dest_tablet_id.is_valid()
      || !snapshot_scn.is_valid()
      || !dest_reorg_scn.is_valid()
      || OB_INVALID_ID == tenant_id
      || 0 == data_version
      || !dest_ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fork ttl reloc args", KR(ret), K(dest_tablet_id), K(snapshot_scn),
        K(dest_reorg_scn), K(tenant_id), K(data_version), K(dest_ls_id));
  } else if (OB_FAIL(dest_tablet_ids.push_back(dest_tablet_id))) {
    LOG_WARN("push fork dest tablet id failed", KR(ret), K(dest_tablet_id));
  } else {
    ObMdsRelocUtil::Param param;
    param.dest_tablet_ids_ = &dest_tablet_ids;
    param.read_snapshot_ = snapshot_scn;
    param.reloc_type_ = ObTabletMdsRelocType::FORK;
    param.dest_transfer_scn_ = dest_reorg_scn;
    param.tenant_id_ = tenant_id;
    param.dest_ls_id_ = dest_ls_id;
    param.src_ls_id_ = source_tablet.get_tablet_meta().ls_id_;
    param.src_tablet_id_ = source_tablet.get_tablet_meta().tablet_id_;
    param.src_reorg_scn_ = source_tablet.get_reorganization_scn();
    // Fork persists its data-format version in the DDL task and the FORK_DST
    // reorg row.  Keep reloc format selection pinned to that version as well;
    // consulting the live tenant version could switch an in-flight fork from
    // V1 to V2 while an upgrade is advancing.
    param.is_reloc_v2_enabled_ = ObMdsRelocUtil::is_reloc_v2_enabled(data_version);
    if (OB_FAIL(ObMdsRelocUtil::submit_tablet_logical_reloc_in_trans(
            source_tablet, param))) {
      LOG_WARN("submit fork ttl logical reloc failed", KR(ret), K(dest_tablet_id),
          K(dest_ls_id), K(snapshot_scn), K(dest_reorg_scn),
          K(data_version), "is_reloc_v2_enabled", param.is_reloc_v2_enabled_);
    }
  }
  return ret;
}

bool ObMdsRelocUtil::is_reloc_v2_enabled(const uint64_t data_version)
{
  // The first supported 4.6.3.1 release includes the V2 reader. Pre-release
  // development snapshots with the same version and only a V1 reader are not
  // supported upgrade sources. Keep writing V1 below this release barrier.
  return GCTX.is_shared_storage_mode() && data_version >= DATA_VERSION_4_6_3_1;
}

bool ObTabletSplitMdsReloc::is_reloc_v2_enabled(const uint64_t data_version)
{
  return ObMdsRelocUtil::is_reloc_v2_enabled(data_version);
}

} // namespace storage
} // namespace oceanbase
