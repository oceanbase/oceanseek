/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX MDS

#include "storage/mds_table/ob_ls_mds_tablet_logical_reloc.h"
#include "storage/mds_table/ob_ls_mds_tablet_reloc_batch_arg.h"
#include "storage/mds_table/ob_mds_table.h"
#include "storage/mds_table/ob_mds_type_id_map.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"
#include "storage/tablet/ob_tablet_split_mds_user_data.h"
#include "storage/tablet/ob_tablet_split_info_mds_user_data.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/mds_table/ob_mds_multikey_reader.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_data.h"
#include "storage/high_availability/ob_transfer_struct.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/utility/serialization.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/tx/ob_tx_log.h"

namespace oceanbase
{
namespace storage
{

using namespace share;
using namespace common;

namespace ls_mds_logical_reloc_detail
{

// ======================== Param ========================

struct ObLsMdsTabletLogicalRelocParam final
{
  ObLsMdsTabletLogicalRelocParam()
    : dest_tablet_ids_(nullptr),
      read_snapshot_(),
      reloc_type_(ObLsMdsTabletRelocType::INVALID),
      dest_transfer_scn_(),
      tenant_id_(OB_INVALID_ID),
      dest_ls_id_()
  {}

  bool is_valid() const
  {
    return nullptr != dest_tablet_ids_
        && dest_tablet_ids_->count() > 0
        && read_snapshot_.is_valid()
        && is_valid_reloc_type(reloc_type_)
        && dest_transfer_scn_.is_valid()
        && OB_INVALID_ID != tenant_id_
        && dest_ls_id_.is_valid();
  }

  const ObIArray<ObTabletID> *dest_tablet_ids_;
  SCN read_snapshot_;
  ObLsMdsTabletRelocType reloc_type_;
  SCN dest_transfer_scn_;
  uint64_t tenant_id_;
  ObLSID dest_ls_id_;
};

// ======================== Helpers ========================


int register_reloc_batch_via_inner_sql_conn(
    observer::ObInnerSQLConnection &conn,
    const uint64_t tenant_id,
    ObArenaAllocator &allocator,
    ObLsMdsTabletRelocBatchArg &batch,
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
  } else if (0 == batch.rows_.count() && !batch.is_final_) {
    allocator.reuse();
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
      LOG_WARN("register reloc batch failed", KR(ret), K(tenant_id), K(batch.ls_id_));
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
int flush_before_push_if_needed(
    ObLsMdsTabletRelocBatchArg &batch,
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
      ret = register_reloc_batch_via_inner_sql_conn(conn, tenant_id, allocator, batch,
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
int serialize_reloc_row_payload(
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

// ======================== Reloc reader traits ========================
// Uniform interface: init(tablet, snapshot) → get_next(alloc, k, v, meta) → release(v)
// DummyKey reader: point-get one row at init, get_next returns it once then ITER_END.
// Multi-key reader: wraps scan reader.

template <typename K, typename V, typename Enable = void> struct reloc_reader;

// DummyKey reader: point-get in get_next, yields one row then ITER_END.
template <typename V>
struct reloc_reader<mds::DummyKey, V>
{
  reloc_reader() : mds_table_(nullptr), tablet_id_(), src_reorg_scn_(), snapshot_(), done_(false) {}
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
        LOG_WARN("point-get dummy-key mds failed", KR(ret), K(tablet_id_), K(mds_type_id), "value_type", typeid(V).name());
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
struct reloc_reader<K, V, typename std::enable_if<!std::is_same<K, mds::DummyKey>::value>::type>
{
  int init(ObTablet &tablet, ObMdsTable &, const SCN &snapshot) { return reader_.init(tablet, snapshot); }
  int get_next(ObArenaAllocator &alloc, K &k, V &v, ObMdsValueMeta &m)
  { return reader_.get_next_mds_kv(alloc, k, v, m); }
private:
  ObTabletMDSInfoReader<K, V> reader_;
};

// ======================== Unified submit_reloc_rows_by_type ========================

// All relocatable MDS rows are read at the latest physical snapshot.
// A finite read_snapshot_ is the business snapshot and filters rows committed later.
bool need_filter_row(
    const ObLsMdsTabletLogicalRelocParam &param,
    const ObMdsValueMeta &mds_meta)
{
  return !param.read_snapshot_.is_max()
      && mds_meta.commit_version_ > param.read_snapshot_.get_val_for_tx();
}

template<typename K, typename V>
int submit_reloc_rows_by_type(
    ObTablet &source_tablet,
    ObMdsTable &mds_table,
    const ObLsMdsTabletLogicalRelocParam &param,
    ObArenaAllocator &allocator,
    observer::ObInnerSQLConnection &conn,
    ObLsMdsTabletRelocBatchArg &batch,
    bool &final_batch_registered)
{
  int ret = OB_SUCCESS;
  constexpr uint8_t mds_type_id = static_cast<uint8_t>(mds_type_id_detail::TypeIdOf<K, V>::value);
  reloc_reader<K, V> reader;
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
      ObLsMdsTabletRelocRowStorage row;
      bool need_filter = false;
      ret = reader.get_next(read_allocator, key, value, mds_meta);
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("reloc get_next failed", KR(ret), K(mds_type_id),
                 "key_type", typeid(K).name(), "value_type", typeid(V).name());
      } else if (need_filter_row(param, mds_meta)) {
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
      } else if (OB_FAIL(flush_before_push_if_needed(
              batch, allocator, param.tenant_id_, conn, row_serialize_size,
              final_batch_registered))) {
        LOG_WARN("flush reloc batch before push failed", KR(ret), K(mds_type_id), K(row_serialize_size),
                 "key_type", typeid(K).name(), "value_type", typeid(V).name());
      } else if (OB_FAIL(serialize_reloc_row_payload(key, value, payload_size, allocator, payload))) {
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
  return ret;
}

// ======================== Unified entry ========================

int submit_tablet_logical_reloc_in_trans(
    ObTablet &source_tablet,
    const ObLsMdsTabletLogicalRelocParam &param)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  observer::ObInnerSQLConnection *conn = nullptr;
  ObLSHandle ls_handle;
  ObMdsTable *mds_table = nullptr;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "RelocBatch"));
  ObLsMdsTabletRelocBatchArg batch;
  bool final_batch_registered = false;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical reloc param", KR(ret), K(param.tenant_id_), K(param.dest_ls_id_),
             K(param.read_snapshot_), K(param.reloc_type_), K(param.dest_transfer_scn_));
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
  }

  // Build targets.
  batch.reloc_type_ = param.reloc_type_;
  batch.ls_id_ = param.dest_ls_id_;
  for (int64_t i = 0; OB_SUCC(ret) && i < param.dest_tablet_ids_->count(); ++i) {
    ObLsMdsTabletRelocTarget target;
    target.tablet_id_ = param.dest_tablet_ids_->at(i);
    target.transfer_scn_ = param.dest_transfer_scn_;
    if (OB_FAIL(batch.targets_.push_back(target))) {
      LOG_WARN("push reloc target failed", KR(ret));
    }
  }
  if (OB_SUCC(ret) && ObLsMdsTabletRelocType::FORK == param.reloc_type_
      && OB_FAIL((submit_reloc_rows_by_type<ObTTLFilterInfoKey, ObTTLFilterInfo>(
          source_tablet, *mds_table, param, allocator, *conn, batch, final_batch_registered)))) {
    LOG_WARN("submit fork ttl reloc rows failed", KR(ret));
  }
  // Expand all types from mds_register.h.
#define GENERATE_LS_INNER_MDS_TABLE
#define _GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION, TRANSFER_RELOC, SPLIT_RELOC) \
  if (OB_SUCC(ret)) {                                                                         \
    if constexpr ((TRANSFER_RELOC) || (SPLIT_RELOC)) {                                          \
      if ((ObLsMdsTabletRelocType::TRANSFER == param.reloc_type_ && (TRANSFER_RELOC))          \
          || (ObLsMdsTabletRelocType::SPLIT == param.reloc_type_ && (SPLIT_RELOC))) {          \
        if (OB_FAIL((submit_reloc_rows_by_type<KEY_TYPE, VALUE_TYPE>(                           \
                source_tablet, *mds_table, param, allocator, *conn, batch,                       \
                final_batch_registered)))) {                                                     \
          LOG_WARN("submit reloc rows failed", KR(ret),                                         \
                   "key_type", #KEY_TYPE, "value_type", #VALUE_TYPE);                           \
        }                                                                                       \
      }                                                                                         \
    }                                                                                           \
  }
#include "storage/multi_data_source/compile_utility/mds_register.h"
#undef _GENERATE_MDS_UNIT_
#undef GENERATE_LS_INNER_MDS_TABLE

  const bool need_final_batch =
      ObLsMdsTabletRelocType::TRANSFER == param.reloc_type_
      || ObLsMdsTabletRelocType::FORK == param.reloc_type_;
  // Transfer and fork always end with one final batch.  It may be empty when
  // the source tablet has no relocatable MDS rows.  Keeping this registration
  // at the tail of the transaction makes the final marker share the same MDS
  // context and commit atomically with every preceding data batch.
  if (OB_SUCC(ret) && need_final_batch) {
    batch.is_final_ = true;
    OB_ASSERT(!final_batch_registered);
    if (OB_FAIL(register_reloc_batch_via_inner_sql_conn(
            *conn, param.tenant_id_, allocator, batch, final_batch_registered))) {
      LOG_WARN("register reloc final batch failed", KR(ret), K(param.reloc_type_));
    }
  } else if (OB_SUCC(ret) && batch.rows_.count() > 0) {
    // Split does not publish a final marker in this version.
    if (OB_FAIL(register_reloc_batch_via_inner_sql_conn(
            *conn, param.tenant_id_, allocator, batch, final_batch_registered))) {
      LOG_WARN("flush remaining batch failed", KR(ret));
    }
  }

  // End transaction.
  if (OB_SUCC(ret)) {
    // No multi-data-source registration may be added between this assertion
    // and trans.end(): final must remain the last registration of the txn.
    OB_ASSERT(!need_final_batch || final_batch_registered);
  }
  if (trans.is_started()) {
    const int tmp_ret = trans.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end trans failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

} // namespace ls_mds_logical_reloc_detail

// ======================== Public API ========================

int ObLsMdsTabletTransferReloc::submit_transfer_mds_reloc(
    ObTablet &src_tablet,
    const SCN &dest_transfer_start_scn,
    const uint64_t tenant_id,
    const ObLSID &dest_ls_id)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  ObSEArray<ObTabletID, 1> dest_tablet_ids;
  const ObTabletID tablet_id = src_tablet.get_tablet_meta().tablet_id_;
  if (OB_UNLIKELY(!dest_transfer_start_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dest_transfer_start_scn", KR(ret), K(dest_transfer_start_scn));
  } else if (OB_FAIL(dest_tablet_ids.push_back(tablet_id))) {
    LOG_WARN("push back dest tablet id failed", KR(ret), K(tablet_id));
  } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(false /*get_commit*/, &src_tablet, user_data))) {
    LOG_WARN("get tablet status failed", KR(ret));
  } else {
    const SCN read_snapshot = share::SCN::max_scn();
    ls_mds_logical_reloc_detail::ObLsMdsTabletLogicalRelocParam param;
    param.dest_tablet_ids_ = &dest_tablet_ids;
    param.read_snapshot_ = read_snapshot;
    param.reloc_type_ = ObLsMdsTabletRelocType::TRANSFER;
    param.dest_transfer_scn_ = dest_transfer_start_scn;
    param.tenant_id_ = tenant_id;
    param.dest_ls_id_ = dest_ls_id;
    if (OB_FAIL(ls_mds_logical_reloc_detail::submit_tablet_logical_reloc_in_trans(src_tablet, param))) {
      LOG_WARN("submit transfer logical reloc failed", KR(ret), K(tablet_id), K(dest_ls_id),
               K(dest_transfer_start_scn), K(read_snapshot));
    }
  }
  return ret;
}

int ObLsMdsTabletSplitReloc::submit_split_multikey_mds_reloc(
    ObTablet &source_tablet,
    const ObIArray<ObTabletID> &dest_tablet_ids,
    const SCN &snapshot_scn,
    const uint64_t tenant_id,
    const ObLSID &dest_ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!snapshot_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid snapshot scn for split reloc", KR(ret), K(snapshot_scn));
  } else if (OB_UNLIKELY(0 >= dest_tablet_ids.count() || !dest_ls_id.is_valid() || OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid split reloc args", KR(ret), K(dest_tablet_ids.count()), K(tenant_id), K(dest_ls_id));
  } else {
    ls_mds_logical_reloc_detail::ObLsMdsTabletLogicalRelocParam param;
    param.dest_tablet_ids_ = &dest_tablet_ids;
    param.read_snapshot_ = snapshot_scn;
    param.reloc_type_ = ObLsMdsTabletRelocType::SPLIT;
    param.dest_transfer_scn_ = SCN::min_scn();
    param.tenant_id_ = tenant_id;
    param.dest_ls_id_ = dest_ls_id;
    if (OB_FAIL(ls_mds_logical_reloc_detail::submit_tablet_logical_reloc_in_trans(source_tablet, param))) {
      LOG_WARN("submit split logical reloc failed", KR(ret), K(dest_ls_id), K(snapshot_scn));
    }
  }
  return ret;
}

int ObLsMdsTabletForkReloc::submit_fork_ttl_mds_reloc(
    ObTablet &source_tablet,
    const ObTabletID &dest_tablet_id,
    const SCN &snapshot_scn,
    const SCN &dest_reorg_scn,
    const uint64_t tenant_id,
    const ObLSID &dest_ls_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 1> dest_tablet_ids;
  if (OB_UNLIKELY(!dest_tablet_id.is_valid()
      || !snapshot_scn.is_valid()
      || !dest_reorg_scn.is_valid()
      || OB_INVALID_ID == tenant_id
      || !dest_ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fork ttl reloc args", KR(ret), K(dest_tablet_id), K(snapshot_scn),
        K(dest_reorg_scn), K(tenant_id), K(dest_ls_id));
  } else if (OB_FAIL(dest_tablet_ids.push_back(dest_tablet_id))) {
    LOG_WARN("push fork dest tablet id failed", KR(ret), K(dest_tablet_id));
  } else {
    ls_mds_logical_reloc_detail::ObLsMdsTabletLogicalRelocParam param;
    param.dest_tablet_ids_ = &dest_tablet_ids;
    param.read_snapshot_ = snapshot_scn;
    param.reloc_type_ = ObLsMdsTabletRelocType::FORK;
    param.dest_transfer_scn_ = dest_reorg_scn;
    param.tenant_id_ = tenant_id;
    param.dest_ls_id_ = dest_ls_id;
    if (OB_FAIL(ls_mds_logical_reloc_detail::submit_tablet_logical_reloc_in_trans(
            source_tablet, param))) {
      LOG_WARN("submit fork ttl logical reloc failed", KR(ret), K(dest_tablet_id),
          K(dest_ls_id), K(snapshot_scn), K(dest_reorg_scn));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
