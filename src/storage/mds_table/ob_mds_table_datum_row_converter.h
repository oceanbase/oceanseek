/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TABLE_DATUM_ROW_CONVERTER_H_
#define OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TABLE_DATUM_ROW_CONVERTER_H_

#include "lib/allocator/ob_allocator.h"
#include <type_traits>
#include "common/ob_tablet_id.h"
#include "common/meta_programming/ob_type_traits.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/mds_table/ob_mds_trans_node_value.h"
#include "storage/mds_table/ob_mds_type_registry.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace memtable
{
class ObMemtableDataHeader;
class ObNopBitMap;
} // namespace memtable

namespace blocksstable
{
struct ObRowHeader;
} // namespace blocksstable

namespace storage
{
class ObITableReadInfo;

/// Converts LS aggregate MDS \c ObDatumRow into typed MDS key/value.
/// Row decoding and materialization are implemented locally in this converter.
class ObMdsTableDatumRowConverter
{
public:
  template <typename V, typename = void>
  struct HasOnCommit : std::false_type
  {};

  template <typename V>
  struct HasOnCommit<V,
                     std::void_t<decltype(std::declval<V &>().on_commit(std::declval<const share::SCN &>(),
                                                                         std::declval<const share::SCN &>()))>> : std::true_type
  {};

  /// Decode \c row into typed \p key / \p value and populate \p meta from row columns.
  template <typename K, typename V>
  static int convert_row_to_kv(
      common::ObIAllocator *allocator,
      const blocksstable::ObDatumRow &row,
      K &key,
      V &value,
      ObMdsValueMeta &meta);

  static int convert_memtable_row(
      const memtable::ObMemtableDataHeader &hdr,
      const ObITableReadInfo &read_info,
      blocksstable::ObDatumRow &row,
      memtable::ObNopBitMap &nop_bitmap,
      bool &read_finished,
      const blocksstable::ObRowHeader *&row_header);

  /// Resolve COMMIT_VERSION / COMMIT_SCN on datum row from lock_for_read outcome.
  /// Committed: patch hint + real end_scn; in-flight 2PC: prepare_version + encoded commit_scn.
  /// \p trans_version is the final commit version (only meaningful when tx_state >= COMMIT).
  /// \p prepare_version is the in-flight prepare version exposed via lock_for_read's
  /// prepare_version_ field; used to populate COMMIT_VERSION on uncommitted rows.
  static int resolve_meta_column(
      blocksstable::ObDatumRow &row,
      const transaction::ObTransID &trans_id,
      const int64_t trans_version,
      const int64_t prepare_version,
      const share::SCN &commit_scn,
      const transaction::ObTxState tx_state);

  static int apply_uncommitted_state_to_datum_row(
      blocksstable::ObDatumRow &row,
      const transaction::ObTransID &row_tx_id,
      const transaction::ObTransID &tx_id,
      const share::SCN &prepare_version,
      const transaction::ObTxState &tx_state);

  /// If the row's commit_version signals a pending on_commit patch (negative value),
  /// normalize commit_version/commit_scn from the row and invoke ops.on_commit on value_buf.
  static void patch_value_from_row_if_needed(
      const blocksstable::ObDatumRow &row,
      const ObMdsTypeRegistry::TypeOps &ops,
      void *value_buf);

private:
  static int decode_mds_row_(
      const blocksstable::ObDatumRow &row,
      common::ObTabletID &tablet_id,
      share::SCN &transfer_scn,
      uint8_t &mds_type,
      common::ObString &mds_key,
      common::ObString &user_data,
      ObMdsValueMeta &meta,
      bool &need_commit_patch);
};

template <typename K, typename V>
int ObMdsTableDatumRowConverter::convert_row_to_kv(
    common::ObIAllocator *allocator,
    const blocksstable::ObDatumRow &row,
    K &key,
    V &value,
    ObMdsValueMeta &meta)
{
  int ret = common::OB_SUCCESS;
  common::ObTabletID tablet_id;
  share::SCN transfer_scn;
  uint8_t mds_type = UINT8_MAX;
  common::ObString mds_key;
  common::ObString user_data;
  bool need_commit_patch = false;
  int64_t key_pos = 0;
  int64_t value_pos = 0;
  key.reset();
  value.reset();
  if (OB_FAIL(decode_mds_row_(row, tablet_id, transfer_scn, mds_type, mds_key, user_data, meta, need_commit_patch))) {
    MDS_LOG(WARN, "fail to decode ls aggregate row", K(ret));
  } else if (OB_FAIL(key.mds_deserialize(mds_key.ptr(), mds_key.length(), key_pos))) {
    MDS_LOG(WARN, "fail to deserialize mds key", K(ret));
  } else if constexpr (OB_TRAIT_DEEP_SERIALIZEABLE(V)) {
    if (nullptr == allocator) {
      ret = common::OB_INVALID_ARGUMENT;
      MDS_LOG(ERROR, "mds value deserialize requires allocator", K(ret), K(typeid(V).name()));
    } else if (OB_FAIL(value.deserialize(*allocator, user_data.ptr(), user_data.length(), value_pos))) {
      MDS_LOG(WARN, "fail to deserialize mds value", K(ret));
    }
  } else if constexpr (OB_TRAIT_SERIALIZEABLE(V)) {
    if (OB_FAIL(value.deserialize(user_data.ptr(), user_data.length(), value_pos))) {
      MDS_LOG(WARN, "fail to deserialize mds value", K(ret));
    }
  } else {
    ret = common::OB_NOT_SUPPORTED;
    MDS_LOG(ERROR, "unsupported mds value deserialize", K(ret), K(typeid(V).name()));
  }
  if (OB_SUCC(ret) && need_commit_patch) {
    if constexpr (HasOnCommit<V>::value) {
      value.on_commit(meta.get_commit_version(), meta.get_commit_scn());
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TABLE_DATUM_ROW_CONVERTER_H_
