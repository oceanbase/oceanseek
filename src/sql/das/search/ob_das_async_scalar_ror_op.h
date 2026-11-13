/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_SCALAR_ROR_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_SCALAR_ROR_OP_H_

#include "lib/container/ob_heap.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_disjunction_filter_op.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "share/search_index/ob_search_index_nested_key_list.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/access/ob_async_inv_iterator_pool.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_posting_iter.h"

namespace oceanbase
{
namespace storage
{
class ObAccessService;
}
namespace sql
{

// Leaf operator for one physical segment of an async scalar/search inverted index.
struct ObDASAsyncScalarRorOpParam : public ObIDASSearchOpParam
{
public:
  ObDASAsyncScalarRorOpParam(const ObDASScalarScanCtDef *scan_ctdef,
                             ObDASScalarScanRtDef *scan_rtdef,
                             const int64_t range_idx,
                             const uint64_t index_table_id,
                             const int64_t group_slot,
                             const bool direct_bind)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_ASYNC_SCALAR_ROR),
      scan_ctdef_(scan_ctdef),
      scan_rtdef_(scan_rtdef),
      range_idx_(range_idx),
      index_table_id_(index_table_id),
      group_slot_(group_slot),
      direct_bind_(direct_bind)
  {}
  ~ObDASAsyncScalarRorOpParam() {}

  OB_INLINE const ObDASScalarScanCtDef *get_scan_ctdef() const { return scan_ctdef_; }
  OB_INLINE ObDASScalarScanRtDef *get_scan_rtdef() const { return scan_rtdef_; }
  OB_INLINE int64_t get_range_idx() const { return range_idx_; }
  OB_INLINE uint64_t get_index_table_id() const { return index_table_id_; }
  OB_INLINE int64_t get_group_slot() const { return group_slot_; }
  OB_INLINE bool get_direct_bind() const { return direct_bind_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    UNUSED(children);
    return common::OB_SUCCESS;
  }

private:
  const ObDASScalarScanCtDef *scan_ctdef_;
  ObDASScalarScanRtDef *scan_rtdef_;
  int64_t range_idx_;
  uint64_t index_table_id_;
  int64_t group_slot_;
  bool direct_bind_;
};

class ObDASAsyncScalarRorOp : public ObIDASSearchOp
{
public:

  ObDASAsyncScalarRorOp(ObDASSearchCtx &search_ctx)
    : ObIDASSearchOp(search_ctx),
      arena_allocator_(common::ObMemAttr(MTL_ID(), "DASAsyncScalar")),
      posting_iter_allocator_(common::ObMemAttr(MTL_ID(), "DASAScaIter")),
      scalar_ctdef_(nullptr),
      scalar_rtdef_(nullptr),
      range_idx_(-1),
      inv_idx_tablet_id_(),
      inv_idx_scan_param_(),
      version_range_(),
      index_table_id_(common::OB_INVALID_ID),
      group_slot_(-1),
      direct_bind_(false),
      tsc_service_(nullptr),
      query_flag_(),
      posting_iter_(nullptr),
      posting_entry_(nullptr),
      posting_scan_param_(),
      nested_key_list_reader_(),
      curr_nested_key_(),
      term_key_(),
      is_first_nk_(true),
      is_empty_range_(false),
      direct_cursor_pending_(false),
      direct_pending_rowid_(0),
      inv_idx_scan_param_inited_(false),
      is_inited_(false)
  {
    op_type_ = DAS_SEARCH_OP_ASYNC_SCALAR_ROR;
  }
  virtual ~ObDASAsyncScalarRorOp() {}
  int get_bitmap_input_range(bool &available, uint64_t &lo, uint64_t &hi,
                             int64_t &estimated_count) override;
  ScoreCapability get_score_capability() const override
  {
    return !is_scoring_ ? ScoreCapability::NONE
        : (nullptr != scalar_rtdef_ ? ScoreCapability::CONSTANT : ScoreCapability::ARRAY);
  }
  double get_constant_score() const override
  { return is_scoring_ && nullptr != scalar_rtdef_ ? scalar_rtdef_->get_boost_weight() : 0.0; }

  void close_posting_iter();
  void release_scan_resources() override { close_posting_iter(); }

  bool supports_direct_rowid_drain() const override
  {
    return DAS_SCOPE_BASE == scope_
        && DAS_ROWID_TYPE_UINT64 == get_rowid_type()
        && !is_scoring_
        && !search_ctx_.is_logical_table();
  }

private:
  int init_nested_key_cursor();
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_rescan_segment(const common::ObNewVersionRange &range) override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_to(const ObDASNestedKey &target, ObDASNestedKey &curr_nk, double &score) override;
  int do_next_rowid(ObDASNestedKey &next_nk, double &score) override;
  int do_drain_until(const ObDASRowID &boundary,
                     int64_t capacity,
                     uint64_t *out_ids,
                     double *out_scores,
                     int64_t &count,
                     ObDASRowID &next_id_after,
                     double &next_score_after) override;
  int do_advance_shallow(const ObDASRowID &target,
                         const bool inclusive,
                         const MaxScoreTuple *&max_score_tuple) override;
  int do_calc_max_score(double &threshold) override;
  int do_direct_drain_rowids(
      const uint64_t range_start,
      const uint64_t range_end,
      ObDASRowIDBatchConsumer &consumer,
      ObDASRowID &next_id_after) override;

private:
  int init_inv_idx_scan_param();
  int init_term_key();
  int init_posting_scan_param();
  int open_posting_iter_from_tablet();
  void reset_leaf_runtime_state();
  int fetch_curr(ObDASRowID &curr_id, double &score);
  int fetch_direct_cursor();
  int position_direct_cursor(
      const uint64_t range_start,
      bool &iter_end,
      bool &include_current);

private:
  ObArenaAllocator arena_allocator_;
  ObArenaAllocator posting_iter_allocator_;
  const ObDASScalarScanCtDef *scalar_ctdef_;
  ObDASScalarScanRtDef *scalar_rtdef_;
  int64_t range_idx_;
  common::ObTabletID inv_idx_tablet_id_;
  ObTableScanParam inv_idx_scan_param_;
  common::ObNewVersionRange version_range_;
  uint64_t index_table_id_;
  int64_t group_slot_;
  bool direct_bind_;
  storage::ObAccessService *tsc_service_;
  common::ObQueryFlag query_flag_;
  blocksstable::ObInvSSTablePostingIter *posting_iter_;
  blocksstable::ObInvPostingIterEntry *posting_entry_;
  blocksstable::ObInvPostingScanParam posting_scan_param_;
  share::ObSearchIndexNestedKeyListReader nested_key_list_reader_;
  ObDASNestedKey curr_nested_key_;
  blocksstable::ObDatumRowkey term_key_;
  bool is_first_nk_;
  bool is_empty_range_;
  // The storage view is consumed synchronously. Only the scalar lookahead value
  // survives between direct-drain calls; no pointer into a posting block is retained.
  bool direct_cursor_pending_;
  uint64_t direct_pending_rowid_;
  bool inv_idx_scan_param_inited_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncScalarRorOp);
};

class ObDASAsyncScalarRorUnionOpParam : public ObDASDisjunctionFilterOpParam
{
public:
  ObDASAsyncScalarRorUnionOpParam(
      const common::ObIArray<ObIDASSearchOp *> &ror_ops,
      const ObDASSearchCost lead_cost,
      const double match_score,
      ObDASScalarScanRtDef *scalar_rtdef,
      const uint64_t index_table_id,
      const int64_t group_slot,
      const bool direct_bind,
      const bool direct_drain_enabled,
      const bool independent_group_filter = false)
    : ObDASDisjunctionFilterOpParam(ror_ops, 1, lead_cost, match_score),
      scalar_rtdef_(scalar_rtdef),
      index_table_id_(index_table_id),
      group_slot_(group_slot),
      direct_bind_(direct_bind),
      direct_drain_enabled_(direct_drain_enabled),
      independent_group_filter_(independent_group_filter)
  {}
  ~ObDASAsyncScalarRorUnionOpParam() {}

  ObDASScalarScanRtDef *get_scalar_rtdef() const { return scalar_rtdef_; }
  uint64_t get_index_table_id() const { return index_table_id_; }
  int64_t get_group_slot() const { return group_slot_; }
  bool get_direct_bind() const { return direct_bind_; }
  bool is_direct_drain_enabled() const { return direct_drain_enabled_; }
  bool is_independent_group_filter() const { return independent_group_filter_; }

private:
  ObDASScalarScanRtDef *scalar_rtdef_;
  uint64_t index_table_id_;
  int64_t group_slot_;
  bool direct_bind_;
  bool direct_drain_enabled_;
  bool independent_group_filter_;
};

// A point predicate may expand to an arbitrary number of runtime ranges. This
// union keeps the streaming ROR path and grows only when a rescan needs more
// leaves; leaves left over after a shrink report EOF.
class ObDASAsyncScalarRorUnionOp : public ObDASDisjunctionFilterOp
{
public:
  explicit ObDASAsyncScalarRorUnionOp(ObDASSearchCtx &search_ctx)
    : ObDASDisjunctionFilterOp(search_ctx),
      scalar_rtdef_(nullptr),
      index_table_id_(common::OB_INVALID_ID),
      group_slot_(-1),
      direct_bind_(false),
      direct_drain_enabled_(false),
      independent_group_filter_(false),
      use_single_child_fast_path_(false),
      direct_heap_cmp_(),
      ready_child_heap_(direct_heap_cmp_, &ctx_allocator()),
      heap_initialized_(false),
      direct_single_cursor_()
  {}
  ~ObDASAsyncScalarRorUnionOp() override {}
  int get_bitmap_input_range(bool &available, uint64_t &lo, uint64_t &hi,
                             int64_t &estimated_count) override
  {
    return collect_bitmap_input_range(children_cnt_, available, lo, hi, estimated_count);
  }

  bool supports_direct_rowid_drain() const override
  { return direct_drain_enabled_; }
  bool can_enumerate_rowids() const override
  {
    return !direct_drain_enabled_
        && ObDASDisjunctionFilterOp::can_enumerate_rowids();
  }

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_rescan_segment(const common::ObNewVersionRange &range) override;
  int do_advance_to(
      const ObDASRowID &target,
      ObDASRowID &curr_id,
      double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_to(const ObDASNestedKey &target, ObDASNestedKey &curr_nk, double &score) override;
  int do_next_rowid(ObDASNestedKey &next_nk, double &score) override;
  int do_drain_until(
      const ObDASRowID &boundary,
      int64_t capacity,
      uint64_t *out_ids,
      double *out_scores,
      int64_t &count,
      ObDASRowID &next_id_after,
      double &next_score_after) override;
  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override;
  int do_calc_max_score(double &threshold) override;
  int do_direct_drain_rowids(
      const uint64_t range_start,
      const uint64_t range_end,
      ObDASRowIDBatchConsumer &consumer,
      ObDASRowID &next_id_after) override;
  int ensure_range_children();
  int append_range_children(const common::ObIArray<ObIDASSearchOp *> &children);
  void reset_direct_cursors();
  int drain_active_child(
      const int64_t child_idx,
      ObDASRowID &current_rowid,
      const ObDASRowID &target,
      const ObDASRowID &boundary,
      ObDASRowIDBatchConsumer &consumer);
  void refresh_single_child_fast_path();
  bool use_single_child_fast_path() const { return use_single_child_fast_path_; }

  struct DirectDrainCursor
  {
    DirectDrainCursor() : rowid_(0), child_idx_(-1) {}
    DirectDrainCursor(const uint64_t rowid, const int64_t child_idx)
      : rowid_(rowid), child_idx_(child_idx)
    {}
    TO_STRING_KV(K_(rowid), K_(child_idx));
    uint64_t rowid_;
    int64_t child_idx_;
  };

  class DirectDrainHeapCmp
  {
  public:
    bool operator()(const DirectDrainCursor &left, const DirectDrainCursor &right) const
    {
      // ObBinaryHeap promotes rhs when comparator(lhs, rhs) is true.
      return left.rowid_ > right.rowid_
          || (left.rowid_ == right.rowid_
              && left.child_idx_ > right.child_idx_);
    }
    int get_error_code() const { return OB_SUCCESS; }
  };

private:
  static constexpr int64_t DRAIN_BATCH_SIZE = DAS_SEARCH_ROWID_DRAIN_BATCH_SIZE;
  ObDASScalarScanRtDef *scalar_rtdef_;
  uint64_t index_table_id_;
  int64_t group_slot_;
  bool direct_bind_;
  bool direct_drain_enabled_;
  // An FTS-owned consumer may reset us, but cannot supply our Search version.
  bool independent_group_filter_;
  // Runtime point ranges can grow across rescans, so keep the union node in
  // the plan but bypass its heap when only the placeholder/range-0 leaf is active.
  bool use_single_child_fast_path_;
  DirectDrainHeapCmp direct_heap_cmp_;
  common::ObBinaryHeap<DirectDrainCursor, DirectDrainHeapCmp, 4> ready_child_heap_;
  bool heap_initialized_;
  DirectDrainCursor direct_single_cursor_;
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncScalarRorUnionOp);
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_SCALAR_ROR_OP_H_
