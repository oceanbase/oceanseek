/**
 * Copyright (C) 2026 OceanBase
 * This file contains confidential and proprietary information.
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_SCALAR_RANGE_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_SCALAR_RANGE_OP_H_

#include "sql/das/search/ob_das_scalar_scan_op.h"
#include "share/search_index/ob_search_index_nested_key_list.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_range_iter.h"
#include "storage/access/ob_async_inv_iterator_pool.h"

namespace oceanbase
{
namespace storage
{
class ObAccessService;
}
namespace sql
{

struct ObDASAsyncScalarRangeOpParam : public ObIDASSearchOpParam
{
public:
  ObDASAsyncScalarRangeOpParam(const ObDASScalarScanCtDef *scan_ctdef,
                               ObDASScalarScanRtDef *scan_rtdef,
                               const uint64_t index_table_id,
                               const int64_t group_slot,
                               const bool direct_bind)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_ASYNC_SCALAR_RANGE),
      scan_ctdef_(scan_ctdef),
      scan_rtdef_(scan_rtdef),
      index_table_id_(index_table_id),
      group_slot_(group_slot),
      direct_bind_(direct_bind)
  {}
  ~ObDASAsyncScalarRangeOpParam() {}

  OB_INLINE const ObDASScalarScanCtDef *get_scan_ctdef() const { return scan_ctdef_; }
  OB_INLINE ObDASScalarScanRtDef *get_scan_rtdef() const { return scan_rtdef_; }
  OB_INLINE uint64_t get_index_table_id() const { return index_table_id_; }
  OB_INLINE int64_t get_group_slot() const { return group_slot_; }
  OB_INLINE bool get_direct_bind() const { return direct_bind_; }
  int get_children_ops(common::ObIArray<ObIDASSearchOp *> &children) const override
  {
    UNUSED(children);
    return common::OB_SUCCESS;
  }

private:
  const ObDASScalarScanCtDef *scan_ctdef_;
  ObDASScalarScanRtDef *scan_rtdef_;
  uint64_t index_table_id_;
  int64_t group_slot_;
  bool direct_bind_;
};

// Non-ROR scalar range scan for one physical async-index segment. The caller
// refreshes scalar_rtdef_->key_ranges_ before open/rescan; this leaf only
// consumes the current ranges, while its parent performs one ordering/dedup pass.
class ObDASAsyncScalarRangeOp : public ObDASScalarScanOp
{
public:
  int get_bitmap_input_range(bool &available, uint64_t &lo, uint64_t &hi,
                             int64_t &estimated_count) override;
  explicit ObDASAsyncScalarRangeOp(ObDASSearchCtx &search_ctx)
    : ObDASScalarScanOp(search_ctx),
      range_allocator_(common::ObMemAttr(MTL_ID(), "DASAScaRange")),
      batch_allocator_(common::ObMemAttr(MTL_ID(), "DASAScaBatch")),
      range_iter_allocator_(common::ObMemAttr(MTL_ID(), "DASAScaRIter")),
      current_range_idx_(0),
      inv_idx_tablet_id_(),
      inv_idx_scan_param_(),
      version_range_(),
      index_table_id_(common::OB_INVALID_ID),
      group_slot_(-1),
      direct_bind_(false),
      is_empty_segment_(false),
      tsc_service_(nullptr),
      query_flag_(),
      range_iter_(nullptr),
      range_scan_param_(),
      term_range_(),
      nested_key_list_reader_(),
      curr_nested_key_(),
      curr_nested_doc_id_(0),
      nested_row_ready_(false),
      inv_idx_scan_param_inited_(false),
      is_inited_(false),
      sort_ranges_(OB_MALLOC_NORMAL_BLOCK_SIZE, sort_range_allocator_)
  {
    op_type_ = DAS_SEARCH_OP_ASYNC_SCALAR_RANGE;
  }
  ~ObDASAsyncScalarRangeOp() override {}

  // Return the next BASE-only raw doc-id batch for the current runtime SQL range. The returned
  // storage view remains valid until the next call.
  int get_next_doc_ids(
      const int64_t capacity,
      const uint64_t *&doc_ids,
      int64_t &count);
  // Convert a four-column SQL range to a storage scan range. Preserve complete
  // MIN/MAX doc_id boundaries and project open boundaries to term keys. Nested
  // keys live in posting values rather than async-index rowkeys.
  // The projected range is deep-copied into allocator before from_range(), so
  // the shallow store_rowkey_ does not refer to the temporary projection.
  static int project_term_range(const common::ObNewRange &range,
                                common::ObIAllocator &allocator,
                                blocksstable::ObDatumRange &term_range);
  /**
   * @brief Use the first bottom value to narrow an unread main-document range scan.
   *
   * Example: price ASC, bottom=30, original 10<price<100 -> private range 10<price<=30.
   * For DESC, the same original range becomes 30<=price<100. Equal prices remain for later sort keys.
   * Composite operators select eligible branches; this method checks the leaf and column/path/value match.
   * @param[in] sort_bottom Borrowed leading key of a full TopN; never retained after this call.
   * @param[out] applied True if new ranges were installed, including an empty set; false if skipped.
   * Installed ranges own their encoded bounds independently of the borrowed key.
   * Unsupported bounds leave the original scan unchanged. Allocation, evaluation and reopen errors propagate.
   */
  virtual int try_apply_sort_bottom(const common::ObDatum &sort_bottom, bool &applied) override;

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_rescan_segment(const common::ObNewVersionRange &range) override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override
  {
    UNUSED(target);
    UNUSED(curr_id);
    UNUSED(score);
    return common::OB_NOT_SUPPORTED;
  }
  int do_next_rowid(ObDASRowID &next_id, double &score) override
  {
    UNUSED(next_id);
    UNUSED(score);
    return common::OB_NOT_SUPPORTED;
  }
  int do_next_rowids(int64_t capacity, int64_t &count) override;
  // cut_objects is caller-owned and must remain valid while cut is used to bind the new ranges.
  int check_sort_range_change_scalar_range(int64_t column, const common::ObObj &path,
      const common::ObObj &value, bool ascending, common::ObObj *cut_objects,
      common::ObRowkey &cut, bool &changed);
  int bind_new_range_for_scalar_range(int64_t column, const common::ObObj &path,
      bool ascending, const common::ObRowkey &cut, bool &applied);
  int init_inv_idx_scan_param();
  int init_range_scan_param();
  int open_range_iter_from_tablet();
  int open_next_range_iter();
  // On storage EOF: open the next SQL range, or return OB_ITER_END when exhausted.
  int advance_range_on_iter_end();
  void close_range_iter();
  void release_scan_resources() override
  {
    close_range_iter();
    // Keep read/pruning flags until open/rescan restores the original query ranges.
    sort_ranges_.reset();
    sort_range_allocator_.reset();
  }
  void reset_nested_cursor();
  bool has_current_range() const
  {
    return OB_NOT_NULL(scalar_rtdef_)
        && current_range_idx_ >= 0
        && current_range_idx_ < active_ranges().count();
  }
  /** @brief Returns private pruned ranges when installed, otherwise the borrowed runtime query ranges. */
  const common::ObIArray<common::ObNewRange> &active_ranges() const
  {
    const common::ObIArray<common::ObNewRange> *ranges = nullptr;
    if (has_sort_ranges_) {
      ranges = &sort_ranges_;
    } else {
      ranges = &scalar_rtdef_->key_ranges_;
    }
    return *ranges;
  }
  /** @brief Discards pruning state so an open or rescan starts from the original runtime ranges. */
  void reset_sort_ranges()
  {
    has_sort_ranges_ = false;
    read_started_ = false;
    sort_ranges_.reset();
    sort_range_allocator_.reuse();
  }
  int position_next_nested_posting();
  int get_output_exprs(const common::ObIArray<ObExpr *> *&output_exprs);

private:
  common::ObArenaAllocator range_allocator_;
  common::ObArenaAllocator batch_allocator_;
  common::ObArenaAllocator range_iter_allocator_;
  int64_t current_range_idx_;
  common::ObTabletID inv_idx_tablet_id_;
  ObTableScanParam inv_idx_scan_param_;
  common::ObNewVersionRange version_range_;
  uint64_t index_table_id_;
  int64_t group_slot_;
  bool direct_bind_;
  bool is_empty_segment_;
  storage::ObAccessService *tsc_service_;
  common::ObQueryFlag query_flag_;
  blocksstable::ObInvSSTableRangeIter *range_iter_;
  blocksstable::ObInvRangeScanParam range_scan_param_;
  blocksstable::ObDatumRange term_range_;
  share::ObSearchIndexNestedKeyListReader nested_key_list_reader_;
  common::ObDatum curr_nested_key_;
  uint64_t curr_nested_doc_id_;
  bool nested_row_ready_;
  bool inv_idx_scan_param_inited_;
  bool is_inited_;
  // Private ranges survive range scans and are released when the Group stops using this leaf.
  // Reset the array before reusing its boundary-storage arena.
  common::ObArenaAllocator sort_range_allocator_{common::ObMemAttr(MTL_ID(), "SortPruneRanges")};
  common::ObSEArray<common::ObNewRange, 4, common::ObIAllocator &> sort_ranges_;
  bool has_sort_ranges_ = false; // Selects private ranges even when pruning has made the range set empty.
  bool read_started_ = false; // Prevents hybrid search sort-topN bottom pruning from reopening ranges after reading starts.
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncScalarRangeOp);
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_SCALAR_RANGE_OP_H_
