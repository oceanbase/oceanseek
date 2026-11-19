/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_LOB_COLUMN_ITERATOR_H_
#define OCEANBASE_STORAGE_OB_LOB_COLUMN_ITERATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/hash/ob_hashset.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/tablet/ob_tablet_table_store_iterator.h"

namespace oceanbase
{
namespace storage
{
class ObTablet;
class ObStorageSchema;

// One cache per Tablet scan: share across its versions with the same columns and filter.
// Only completed MAJOR_SSTABLE and INC_MAJOR_SSTABLE files participate in deduplication.
// Use a new cache if collected results are discarded and the scan is restarted.
class ObLobSSTableScanCache final
{
public:
  ObLobSSTableScanCache() = default;

  bool contains(const ObITable::TableKey &key) const;
  int add(const ObITable::TableKey &key);

private:
  static constexpr int64_t INITIAL_BUCKET_COUNT = 128;
  static constexpr int64_t MAX_SSTABLE_COUNT = 10000;
  common::hash::ObHashSet<ObITable::TableKey, common::hash::NoPthreadDefendMode> scanned_sstables_;

  DISALLOW_COPY_AND_ASSIGN(ObLobSSTableScanCache);
};

// Physical SSTable rows of a selected LOB Meta/Ctrl Tablet, projected in request order.
// Excludes Memtables; no snapshot filtering or version fusion.
// The caller must keep the selected Tablet handle alive until the iterator is destroyed.
class ObLobColumnIterator final
{
public:
  ObLobColumnIterator();
  ~ObLobColumnIterator();
  // Initialize a scan of the selected Tablet's SSTables for the requested columns.
  // skip_uncommitted filters the stored row flag only; it does not resolve transaction state.
  int init(
      const ObTablet &tablet,
      const common::ObIArray<share::schema::ObColDesc> &columns,
      const bool skip_uncommitted);
  // Initialize with a shared cache that skips completed SSTables and must outlive this iterator.
  int init_with_scan_cache(
      const ObTablet &tablet,
      const common::ObIArray<share::schema::ObColDesc> &columns,
      const bool skip_uncommitted,
      ObLobSSTableScanCache &scan_cache);
  // Preserve SQL NULL for caller-side validation; skip all-NOP projections, reject partial NOP.
  // Historical and DELETE rows are not fused or filtered; uncommitted rows follow the init option.
  // OB_ITER_END means all SSTables are exhausted; row data lasts until the next call or destruction.
  // Stop and destroy the iterator on errors.
  int get_next_row(const blocksstable::ObDatumRow *&row);

  // SSTable elapsed time in microseconds, including caller work between rows.
  // Accumulated only when each SSTable finishes successfully.
  int64_t major_scan_cost_us_; // Includes MAJOR_SSTABLE and INC_MAJOR_SSTABLE.
  int64_t minor_scan_cost_us_;
  int64_t mini_scan_cost_us_;
  int64_t other_scan_cost_us_;
  // Meta rows counted by GC callers after successful reference decoding.
  int64_t outtable_row_count_;
  int64_t non_outtable_row_count_;

private:
  void update_scan_cost_();
  int prepare_columns_(const ObStorageSchema &schema,
      const common::ObIArray<share::schema::ObColDesc> &columns);
  int open_next_sstable_();
  // Advances to the next data macro, switching SSTables as needed.
  int open_next_macro_();
  int project_row_(const blocksstable::ObDatumRow &source, bool &skip);

private:
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator macro_allocator_;
  common::ObSEArray<share::schema::ObColDesc, 8> columns_;
  common::ObSEArray<int64_t, 8> projector_;
  ObTableStoreIterator tables_;
  blocksstable::ObSSTableMetaHandle sstable_meta_;
  blocksstable::ObMacroIdIterator macro_ids_;
  blocksstable::ObMacroBlockRowBareIterator *macro_iter_;
  blocksstable::ObDatumRow row_;
  int64_t column_count_;
  int64_t rowkey_count_;
  int64_t sstable_scan_start_us_;
  const blocksstable::ObSSTable *sstable_;
  ObLobSSTableScanCache *scan_cache_;
  bool has_macro_;
  bool skip_uncommitted_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObLobColumnIterator);
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_LOB_COLUMN_ITERATOR_H_
