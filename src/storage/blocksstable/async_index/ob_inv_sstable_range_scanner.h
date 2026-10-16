/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_RANGE_SCANNER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_RANGE_SCANNER_H_

#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/access/ob_store_row_iterator.h"

namespace oceanbase
{
namespace blocksstable
{

// =============================================================================
// Range scanner — iterate (term, doc_id) pairs within an inverted index range
//
// Inherits ObStoreRowIterator so it can be returned from ObITable::scan().
// Iterates in dictionary order: terms sorted lexicographically, within each
// term doc_ids monotonically increasing. Doc_id may decrease at term transitions.
//
// Row format follows the read_info projection:
//   (term_column, doc_id_column, [tf, doc_length, position_list, ...])
//
// Usage:
//   ObInvSSTableRangeScanner scanner;
//   scanner.init(param, context, table, &range);
//   const ObDatumRow *row = nullptr;
//   while (OB_SUCC(scanner.get_next_row(row))) {
//     // process row
//   }
// =============================================================================
class ObInvSSTableRangeScanner : public storage::ObStoreRowIterator
{
public:
  ObInvSSTableRangeScanner();
  virtual ~ObInvSSTableRangeScanner();
  /// Open a range scanner for an inverted index key range.
  /// Locates the starting position via term index lookup and positions
  /// the cursor at the first (term, doc_id) >= range.start_key.
  /// @param sstable    the inverted index SSTable to scan
  /// @param allocator  allocator for internal buffers (borrowed, not owned)
  /// @param read_info  schema / column info for key comparison
  /// @param range      [start_key, end_key) — both sides can be min/max
  int open(
    ObInvSSTable &sstable,
    common::ObIAllocator &allocator,
    const ObITableReadInfo &read_info,
    const ObDatumRange &range);

  /// Reset the scanner to initial state.
  virtual void reset() override;
  /// Reuse the scanner for a new scan (lightweight reset).
  virtual void reuse() override;
  /// Get the next (term, doc_id) row. Returns OB_ITER_END when exhausted.
  virtual int get_next_row(const blocksstable::ObDatumRow *&row) override;

  /// Batch iteration and vectorized interface.
  virtual int get_next_rows() override;

protected:
  ObDatumRange scan_range_;
  bool is_end_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_RANGE_SCANNER_H_
