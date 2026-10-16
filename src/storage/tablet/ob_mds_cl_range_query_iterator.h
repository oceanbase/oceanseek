/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_MDS_CL_RANGE_QUERY_ITERATOR
#define OCEANBASE_STORAGE_OB_MDS_CL_RANGE_QUERY_ITERATOR

#include "storage/tablet/ob_mds_range_query_iterator.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/mds_table/ob_mds_table.h"
#include "storage/mds_table/ob_mds_table_datum_row_converter.h"
#include "storage/mds_table/ob_mds_table_scan_iterator.h"
#include "storage/mds_table/ob_mds_tablet_schema.h"
#include "storage/tx_storage/ob_ls_handle.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace storage
{
class ObTabletHandle;
class ObTabletPointer;
class ObTablet;

template <typename K, typename T>
class ObMdsRangeQueryIterator
{
public:
  ObMdsRangeQueryIterator();
  ~ObMdsRangeQueryIterator();
  ObMdsRangeQueryIterator(const ObMdsRangeQueryIterator&) = delete;
  ObMdsRangeQueryIterator &operator=(const ObMdsRangeQueryIterator&) = delete;
public:
  int init(
      const share::SCN &read_snapshot,
      const ObTabletHandle &tablet_handle,
      const ObTabletHandle &src_tablet_handle);
  /// Deserialize next merged row into typed `K` / `T` (`user_allocator` is used for value payload).
  /// Caller must reset @p key / @p value / @p meta before each call; outputs are valid only on OB_SUCCESS.
  int get_next(common::ObIAllocator &user_allocator, K &key, T &value, ObMdsValueMeta &meta);
private:
  int open_mds_scan_(
      const share::SCN &read_snapshot,
      const ObTabletHandle &user_tablet_handle,
      ObLSHandle &out_ls_handle,
      ObMdsTableScanIterator &out_scan_iter);
  int load_side_row_(
      ObMdsTableScanIterator &scan_iter,
      bool &side_finished,
      bool &side_row_valid,
      blocksstable::ObDatumRow *&side_row_ptr);
  int compare_side_row_keys_(const blocksstable::ObDatumRow &a, const blocksstable::ObDatumRow &b, int &cmp) const;
  int emit_row_copy_(const blocksstable::ObDatumRow &from, blocksstable::ObDatumRow &out);
  /// Merge cur/src LS aggregate scans; deep-copy one chosen row into `out` (storage via `row_copy_allocator_`).
  int merge_next_chosen_datum_row_(blocksstable::ObDatumRow &out);
  bool is_inited_;
  common::ObArenaAllocator scan_allocator_;
  common::ObArenaAllocator row_copy_allocator_;
  bool cur_finished_;
  bool src_finished_;
  bool cur_row_valid_;
  bool src_row_valid_;
  /// Current row from each scan merge; invalidated after that side's next ObMdsTableScanIterator::get_next_row.
  blocksstable::ObDatumRow *cur_datum_row_ptr_;
  blocksstable::ObDatumRow *src_datum_row_ptr_;
  ObLSHandle cur_ls_handle_;
  ObLSHandle src_ls_handle_;
  ObMdsTableScanIterator cur_mds_scan_iter_;
  ObMdsTableScanIterator src_mds_scan_iter_;
};

template <typename K, typename T>
ObMdsRangeQueryIterator<K, T>::ObMdsRangeQueryIterator()
  : is_inited_(false),
    scan_allocator_(lib::ObMemAttr(MTL_ID(), "MdsRangeScan")),
    row_copy_allocator_(lib::ObMemAttr(MTL_ID(), "MdsRangeRow")),
    cur_finished_(false),
    src_finished_(false),
    cur_row_valid_(false),
    src_row_valid_(false),
    cur_datum_row_ptr_(nullptr),
    src_datum_row_ptr_(nullptr),
    cur_ls_handle_(),
    src_ls_handle_(),
    cur_mds_scan_iter_(),
    src_mds_scan_iter_()
{
}

template <typename K, typename T>
ObMdsRangeQueryIterator<K, T>::~ObMdsRangeQueryIterator()
{
  src_mds_scan_iter_.reset();
  cur_mds_scan_iter_.reset();
  src_ls_handle_.reset();
  cur_ls_handle_.reset();
  is_inited_ = false;
  cur_finished_ = false;
  src_finished_ = false;
  cur_row_valid_ = false;
  src_row_valid_ = false;
  cur_datum_row_ptr_ = nullptr;
  src_datum_row_ptr_ = nullptr;
  row_copy_allocator_.reset();
  scan_allocator_.reset();
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::init(
    const share::SCN &read_snapshot,
    const ObTabletHandle &tablet_handle,
    const ObTabletHandle &src_tablet_handle)
{
  int ret = common::OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    MDS_LOG(WARN, "this iter has been", K(ret), KPC(tablet_handle.get_obj()));
  } else {
    cur_finished_ = false;
    src_finished_ = false;
    cur_row_valid_ = false;
    src_row_valid_ = false;
    cur_datum_row_ptr_ = nullptr;
    src_datum_row_ptr_ = nullptr;
    if (OB_UNLIKELY(!read_snapshot.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      MDS_LOG(WARN, "invalid read snapshot", KR(ret), K(read_snapshot));
    } else if (OB_FAIL(open_mds_scan_(
                   read_snapshot, tablet_handle, cur_ls_handle_, cur_mds_scan_iter_))) {
      MDS_LOG(WARN, "failed to init cur mds table scan", K(ret), KPC(tablet_handle.get_obj()));
    } else if (OB_ISNULL(src_tablet_handle.get_obj())) {
      src_finished_ = true;
    } else {
      src_finished_ = false;
      if (OB_FAIL(open_mds_scan_(
            read_snapshot, src_tablet_handle, src_ls_handle_, src_mds_scan_iter_))) {
        MDS_LOG(WARN, "init src mds table scan failed ", K(ret), K(src_tablet_handle), KPC(src_tablet_handle.get_obj()));
        cur_mds_scan_iter_.reset();
        cur_ls_handle_.reset();
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }

  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::open_mds_scan_(
    const share::SCN &read_snapshot,
    const ObTabletHandle &user_tablet_handle,
    ObLSHandle &out_ls_handle,
    ObMdsTableScanIterator &out_scan_iter)
{
  int ret = OB_SUCCESS;
  bool is_mds_data_complete = false;
  ObMdsTable *mds_table = nullptr;
  ObTablet *tablet = user_tablet_handle.get_obj();
  if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "tablet is null", K(ret));
  } else if (OB_UNLIKELY(!read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "invalid read snapshot", K(ret), K(read_snapshot));
  } else if (OB_FAIL(ObMdsRangeQueryIteratorHelper::check_mds_data_complete(user_tablet_handle, is_mds_data_complete))) {
    MDS_LOG(WARN, "failed to check mds data is complete or not.");
  } else if (!is_mds_data_complete) {
    ret = OB_EAGAIN;
    MDS_LOG(INFO, "MDS data is incomplete. Please try again later to retrieve the full dataset.", K(ret));
  } else if (OB_UNLIKELY(!ObMdsTabletSchemaHelper::get_instance().is_inited())) {
    ret = OB_NOT_INIT;
    MDS_LOG(WARN, "mds tablet schema helper not inited", K(ret));
  } else if (OB_FAIL(tablet->get_mds_table(out_ls_handle, mds_table))) {
    MDS_LOG(WARN, "fail to get ls aggregate mds table", K(ret));
  } else if (OB_ISNULL(mds_table)) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "mds table is null", K(ret));
  } else if (OB_FAIL((mds_table->table_scan<K, T>(scan_allocator_,
                        tablet->get_tablet_id(),
                        tablet->get_reorganization_scn(),
                        read_snapshot,
                        out_scan_iter)))) {
    MDS_LOG(WARN, "fail to open mds table scan", K(ret));
  }
  if (OB_FAIL(ret)) {
    out_scan_iter.reset();
    out_ls_handle.reset();
  }
  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::load_side_row_(
    ObMdsTableScanIterator &scan_iter,
    bool &side_finished,
    bool &side_row_valid,
    blocksstable::ObDatumRow *&side_row_ptr)
{
  int ret = common::OB_SUCCESS;
  if (side_finished || side_row_valid) {
  } else {
    side_row_ptr = nullptr;
    if (OB_FAIL(scan_iter.get_next_row(side_row_ptr))) {
      if (common::OB_ITER_END == ret) {
        side_finished = true;
        side_row_valid = false;
        side_row_ptr = nullptr;
        ret = common::OB_SUCCESS;
      } else {
        MDS_LOG(WARN, "fail to get next datum row from scan iter", K(ret));
      }
    } else if (OB_ISNULL(side_row_ptr)) {
      ret = common::OB_ERR_UNEXPECTED;
      MDS_LOG(WARN, "datum row is null", K(ret), KP(side_row_ptr));
    } else {
      side_row_valid = true;
    }
  }
  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::compare_side_row_keys_(
    const blocksstable::ObDatumRow &a,
    const blocksstable::ObDatumRow &b,
    int &cmp) const
{
  int ret = common::OB_SUCCESS;
  cmp = 0;
  const int64_t cca = a.get_column_count();
  const int64_t ccb = b.get_column_count();
  if (OB_UNLIKELY(cca != ccb)) {
    ret = common::OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "column count mismatch", K(ret), K(cca), K(ccb));
  } else if (!ObMdsTabletSchemaDef::is_known_mds_row_layout(cca)) {
    ret = common::OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "ls aggregate row column count mismatch", K(ret), K(cca));
  } else {
  const int64_t key_idx = ObMdsTabletSchemaDef::get_phy_col_idx(
      cca, ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX::MDS_KEY);
    const common::ObString sa = a.storage_datums_[key_idx].get_string();
    const common::ObString sb = b.storage_datums_[key_idx].get_string();
    cmp = sa.compare(sb);
  }
  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::emit_row_copy_(
    const blocksstable::ObDatumRow &from,
    blocksstable::ObDatumRow &out)
{
  int ret = common::OB_SUCCESS;
  const int64_t col_cnt = from.get_column_count();
  if (!out.is_valid() || out.get_capacity() < col_cnt) {
    out.reset();
    if (OB_FAIL(out.init(col_cnt))) {
      MDS_LOG(WARN, "fail to init out datum row", K(ret));
    }
  } else {
    out.reuse();
  }
  if (OB_SUCC(ret) && OB_FAIL(out.deep_copy(from, row_copy_allocator_))) {
    MDS_LOG(WARN, "fail to deep copy chosen datum row", K(ret));
  }
  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::merge_next_chosen_datum_row_(blocksstable::ObDatumRow &out)
{
  int ret = common::OB_SUCCESS;
  int compare_result = 0;
  out.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    MDS_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(load_side_row_(cur_mds_scan_iter_, cur_finished_, cur_row_valid_, cur_datum_row_ptr_))) {
    MDS_LOG(WARN, "fail to load cur side row", K(ret));
  } else if (OB_FAIL(load_side_row_(src_mds_scan_iter_, src_finished_, src_row_valid_, src_datum_row_ptr_))) {
    MDS_LOG(WARN, "fail to load src side row", K(ret));
  } else if (cur_finished_ && src_finished_) {
    ret = common::OB_ITER_END;
    MDS_LOG(DEBUG, "both iter end", K(ret));
  } else if (cur_finished_) {
    if (OB_UNLIKELY(!src_row_valid_ || OB_ISNULL(src_datum_row_ptr_))) {
      ret = common::OB_ERR_UNEXPECTED;
      MDS_LOG(WARN, "src row invalid", K(ret), K(src_row_valid_), KP(src_datum_row_ptr_));
    } else if (OB_FAIL(emit_row_copy_(*src_datum_row_ptr_, out))) {
      MDS_LOG(WARN, "fail to emit src row", K(ret));
    } else {
      src_row_valid_ = false;
      src_datum_row_ptr_ = nullptr;
    }
  } else if (src_finished_) {
    if (OB_UNLIKELY(!cur_row_valid_ || OB_ISNULL(cur_datum_row_ptr_))) {
      ret = common::OB_ERR_UNEXPECTED;
      MDS_LOG(WARN, "cur row invalid", K(ret), K(cur_row_valid_), KP(cur_datum_row_ptr_));
    } else if (OB_FAIL(emit_row_copy_(*cur_datum_row_ptr_, out))) {
      MDS_LOG(WARN, "fail to emit cur row", K(ret));
    } else {
      cur_row_valid_ = false;
      cur_datum_row_ptr_ = nullptr;
    }
  } else if (OB_UNLIKELY(!cur_row_valid_ || !src_row_valid_)) {
    ret = common::OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "both sides should have valid row", K(ret), K(cur_row_valid_), K(src_row_valid_));
  } else if (OB_UNLIKELY(OB_ISNULL(cur_datum_row_ptr_) || OB_ISNULL(src_datum_row_ptr_))) {
    ret = common::OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "unexpected null side row ptr", K(ret), KP(cur_datum_row_ptr_), KP(src_datum_row_ptr_));
  } else if (OB_FAIL(compare_side_row_keys_(*src_datum_row_ptr_, *cur_datum_row_ptr_, compare_result))) {
    MDS_LOG(WARN, "fail to compare row keys", K(ret));
  } else if (compare_result < 0) {
    if (OB_FAIL(emit_row_copy_(*src_datum_row_ptr_, out))) {
      MDS_LOG(WARN, "fail to emit src row", K(ret));
    } else {
      src_row_valid_ = false;
      src_datum_row_ptr_ = nullptr;
    }
  } else if (compare_result == 0) {
    if (OB_FAIL(emit_row_copy_(*cur_datum_row_ptr_, out))) {
      MDS_LOG(WARN, "fail to emit cur row", K(ret));
    } else {
      cur_row_valid_ = false;
      src_row_valid_ = false;
      cur_datum_row_ptr_ = nullptr;
      src_datum_row_ptr_ = nullptr;
      MDS_LOG(WARN, "dest_tablet and src_tablet in a transfer progress should not have the same Key",
          K(ret), K(lbt()));
    }
  } else {
    if (OB_FAIL(emit_row_copy_(*cur_datum_row_ptr_, out))) {
      MDS_LOG(WARN, "fail to emit cur row", K(ret));
    } else {
      cur_row_valid_ = false;
      cur_datum_row_ptr_ = nullptr;
    }
  }
  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::get_next(
    common::ObIAllocator &user_allocator,
    K &key,
    T &value,
    ObMdsValueMeta &meta)
{
  int ret = common::OB_SUCCESS;
  row_copy_allocator_.reuse();
  blocksstable::ObDatumRow chosen_row;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    MDS_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(merge_next_chosen_datum_row_(chosen_row))) {
    if (common::OB_ITER_END != ret) {
      MDS_LOG(WARN, "fail to merge next mds datum row", K(ret));
    }
  } else if (OB_FAIL(ObMdsTableDatumRowConverter::convert_row_to_kv(&user_allocator, chosen_row, key, value, meta))) {
    MDS_LOG(WARN, "fail to convert datum row to typed key/value with meta", K(ret));
  }
  chosen_row.reset();
  return ret;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_CL_RANGE_QUERY_ITERATOR
