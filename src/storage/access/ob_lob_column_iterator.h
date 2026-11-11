/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_LOB_COLUMN_ITERATOR_H_
#define OCEANBASE_STORAGE_OB_LOB_COLUMN_ITERATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/tablet/ob_tablet_table_store_iterator.h"

namespace oceanbase
{
namespace storage
{
class ObTablet;
class ObStorageSchema;

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
  // Preserve SQL NULL for caller-side validation; skip all-NOP projections, reject partial NOP.
  // Historical and DELETE rows are not fused or filtered; uncommitted rows follow the init option.
  // OB_ITER_END means all SSTables are exhausted; row data lasts until the next call or destruction.
  // Stop and destroy the iterator on errors.
  int get_next_row(const blocksstable::ObDatumRow *&row);

private:
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
  const blocksstable::ObSSTable *sstable_;
  bool has_macro_;
  bool skip_uncommitted_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObLobColumnIterator);
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_LOB_COLUMN_ITERATOR_H_
