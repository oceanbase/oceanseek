/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


/*
 * Overview
 * - Abstract base class for DAS search operators.
 *
 * Key Responsibilities
 * - Lifecycle management: open / rescan / close.
 * - Iterative APIs: advance_to, advance_shallow, next_rowid, and optional next_rowids (batch).
 * - Context access: retrieving rowid metadata, eval ctx, max batch size, etc.
 * - Operator identity and children management for composite search plans.
 */
#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_I_DAS_SEARCH_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_I_DAS_SEARCH_OP_H_

#include "sql/das/search/ob_das_search_define.h"
#include "sql/das/search/ob_das_search_context.h"
#include "common/ob_range.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "lib/container/ob_fixed_array.h"
#include "share/diagnosis/ob_runtime_profile.h"

namespace oceanbase
{
namespace common
{
enum class ObProfileId;
}
namespace sql
{

class ObDASSearchCtx;
class DocScoreAccumulator;

/*
 * MaxScoreTuple represents a [mid_id, max_id] interval and its score upper bound.
 * It is returned by the score-contributing operators to the upper layer for score-based dynamic pruning.
*/
struct MaxScoreTuple
{
public:
  MaxScoreTuple()
    : min_id_(),
      max_id_(),
      max_score_(0.0)
  { }

public:
  OB_INLINE void reset()
  {
    min_id_.reset();
    max_id_.reset();
    max_score_ = 0.0;
  }

  OB_INLINE void set(const ObDASRowID &min_id, const ObDASRowID &max_id, double max_score)
  {
    min_id_ = min_id;
    max_id_ = max_id;
    max_score_ = max_score;
  }

  OB_INLINE const ObDASRowID &get_min_id() const { return min_id_; }
  OB_INLINE const ObDASRowID &get_max_id() const { return max_id_; }
  OB_INLINE double get_max_score() const { return max_score_; }

private:
  ObDASRowID min_id_;
  ObDASRowID max_id_;
  double max_score_;
};

// Consumes uint64 RowID batches without requiring the producer to expose an
// ordered RowID stream. Bitmap consumers resolve both ordering and duplicates.
class ObDASRowIDBatchConsumer
{
public:
  ObDASRowIDBatchConsumer() {}
  virtual ~ObDASRowIDBatchConsumer() {}
  virtual int consume(const uint64_t *rowids, const int64_t count) = 0;
};

// Shared batch size for RowID drain producers and consumers.
static constexpr int64_t DAS_SEARCH_ROWID_DRAIN_BATCH_SIZE = 256;

enum ObDASSearchOpType
{
  DAS_SEARCH_OP_CONJUNCTION = 0,
  DAS_SEARCH_OP_DISJUNCTION,
  DAS_SEARCH_OP_DISJUNCTION_FILTER,
  DAS_SEARCH_OP_REQ_OPT,
  DAS_SEARCH_OP_REQ_EXCL,
  DAS_SEARCH_OP_SCALAR_INDEX_ROR,
  DAS_SEARCH_OP_SCALAR_PRIMARY_ROR,
  DAS_SEARCH_OP_SCALAR_SCAN,
  DAS_SEARCH_OP_BITMAP,
  DAS_SEARCH_OP_SORT,
  DAS_SEARCH_OP_TOPK_COLLECT,
  DAS_SEARCH_OP_DISJUNCTIVE_MAX,
  DAS_SEARCH_OP_TOKEN,
  DAS_SEARCH_OP_MATCH_PHRASE,
  DAS_SEARCH_OP_DUMMY,
  DAS_SEARCH_OP_BMW,
  DAS_SEARCH_OP_BMM,
  DAS_SEARCH_OP_DELETION,
  DAS_SEARCH_OP_ASYNC_TOKEN,
  DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE,
  DAS_SEARCH_OP_ASYNC_SCALAR_ROR,
  DAS_SEARCH_OP_NESTED, // FARM COMPAT WHITELIST
  DAS_SEARCH_OP_ASYNC_SCALAR_RANGE, // FARM COMPAT WHITELIST
  DAS_SEARCH_OP_ASYNC_SEGMENT_GROUP_MERGE,
  // ...TBD
  DAS_SEARCH_OP_MAX
};

struct ObDASSearchWorkEstimate
{
public:
  ObDASSearchWorkEstimate()
    : ror_op_count_(0),
      known_ror_work_row_count_(0),
      unknown_ror_op_count_(0)
  {}

  void add_ror_work(const int64_t work_row_count)
  {
    ror_op_count_ = ror_op_count_ == INT64_MAX
        ? INT64_MAX
        : ror_op_count_ + 1;
    if (work_row_count < 0) {
      unknown_ror_op_count_ = unknown_ror_op_count_ == INT64_MAX
          ? INT64_MAX
          : unknown_ror_op_count_ + 1;
    } else {
      known_ror_work_row_count_ =
          work_row_count > INT64_MAX - known_ror_work_row_count_
              ? INT64_MAX
              : known_ror_work_row_count_ + work_row_count;
    }
  }

  int64_t get_ror_work_row_count() const
  {
    return unknown_ror_op_count_ > 0
        ? -1
        : known_ror_work_row_count_;
  }

  int64_t ror_op_count_;
  int64_t known_ror_work_row_count_;
  int64_t unknown_ror_op_count_;

  TO_STRING_KV(K_(ror_op_count), K_(known_ror_work_row_count),
               K_(unknown_ror_op_count), "ror_work_row_count",
               get_ror_work_row_count());
};

/*
 * RowIDStore - Batch storage for ObDASRowID, materializing rowids from expressions.
 */
class RowIDStore
{
friend class Iterator;
public:
  class Iterator
  {
  public:
    Iterator() : cur_idx_(-1), store_(nullptr) {}
    Iterator(const RowIDStore *store) : cur_idx_(-1), store_(store) {}
    ~Iterator() { reset(); }

    void init(const RowIDStore *store) { store_ = store; cur_idx_ = -1; }
    void reset() { cur_idx_ = -1; store_ = nullptr; }
    void reuse() { cur_idx_ = 0; }
    OB_INLINE bool is_empty() const { return nullptr == store_ || cur_idx_ < 0 || cur_idx_ >= store_->count(); }
    OB_INLINE int64_t cur_idx() const { return cur_idx_; }
    OB_INLINE int64_t count() const { return nullptr == store_ ? 0 : store_->count(); }
    OB_INLINE void next_idx() { ++ cur_idx_; }
    OB_INLINE int get_cur_rowid(ObDASRowID &rowid) const
    {
      return (nullptr != store_) ? store_->get_rowid(cur_idx_, rowid) : OB_NOT_INIT;
    }
    OB_INLINE int lower_bound(const ObDASRowID &target)
    {
      return (nullptr != store_) ? store_->lower_bound(target, cur_idx_, cur_idx_) : OB_NOT_INIT;
    }
    OB_INLINE int upper_bound(const ObDASRowID &target)
    {
      return (nullptr != store_) ? store_->upper_bound(target, cur_idx_, cur_idx_) : OB_NOT_INIT;
    }
    OB_INLINE int drain_until(
      const ObDASRowID &boundary,
      int64_t capacity,
      uint64_t *out_ids,
      double *out_scores,
      int64_t &count,
      ObDASRowID &next_id_after,
      double &next_score_after,
      bool &drained)
    {
      return (nullptr != store_) ? store_->drain_until(
        cur_idx_, boundary, capacity, out_ids, out_scores,
        count, next_id_after, next_score_after, drained) : OB_NOT_INIT;
    }

  private:
    int64_t cur_idx_;
    const RowIDStore *store_;
  };

  explicit RowIDStore(ObDASSearchCtx &search_ctx, ObDASScopeId scope = DAS_SCOPE_BASE)
    : search_ctx_(search_ctx), scope_(scope) {}
  virtual ~RowIDStore() = default;

  virtual int init(int64_t capacity) = 0;
  // fill the rowid store with the rowids from the expressions with range [begin_idx, end_idx)
  virtual int fill(const int64_t begin_idx_inclusive, const int64_t end_idx_exclusive, const common::ObIArray<ObExpr *> &rowid_exprs) = 0;
  virtual void reset() = 0;
  virtual void reuse() = 0;
  virtual int64_t count() const = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;

private:
  virtual int get_rowid(int64_t idx, ObDASRowID &rowid) const = 0;
  virtual int lower_bound(const ObDASRowID &target, const int64_t start_idx, int64_t &idx) const = 0;
  virtual int upper_bound(const ObDASRowID &target, const int64_t start_idx, int64_t &idx) const
  { return OB_NOT_IMPLEMENT; }
  virtual int drain_until(
      int64_t &iter_idx,
      const ObDASRowID &boundary,
      int64_t capacity,
      uint64_t *out_ids,
      double *out_scores,
      int64_t &count,
      ObDASRowID &next_id_after,
      double &next_score_after,
      bool &drained) const
  { return OB_NOT_IMPLEMENT; }

protected:
  ObDASSearchCtx &search_ctx_;
  ObDASScopeId scope_;
};

class UInt64RowIDStore : public RowIDStore
{
public:
  explicit UInt64RowIDStore(ObDASSearchCtx &search_ctx, ObDASScopeId scope = DAS_SCOPE_BASE)
    : RowIDStore(search_ctx, scope), data_(nullptr), capacity_(0), count_(0) {}
  ~UInt64RowIDStore() override { reset(); }

  int init(int64_t capacity) override;
  int fill(const int64_t begin_idx_inclusive, const int64_t end_idx_exclusive, const common::ObIArray<ObExpr *> &rowid_exprs) override;
  void reset() override;
  void reuse() override { count_ = 0; }
  int64_t count() const override { return count_; }

  TO_STRING_KV(KP_(data), K_(capacity), K_(count));

private:
  virtual int get_rowid(int64_t idx, ObDASRowID &rowid) const override;
  virtual int lower_bound(const ObDASRowID &target, const int64_t start_idx, int64_t &idx) const override;
  virtual int upper_bound(const ObDASRowID &target, const int64_t start_idx, int64_t &idx) const override;
  int drain_until(
      int64_t &iter_idx,
      const ObDASRowID &boundary,
      int64_t capacity,
      uint64_t *out_ids,
      double *out_scores,
      int64_t &count,
      ObDASRowID &next_id_after,
      double &next_score_after,
      bool &drained) const override;

private:
  uint64_t *data_;
  int64_t capacity_;
  int64_t count_;
};

class CompactRowIDStore : public RowIDStore
{
public:
  explicit CompactRowIDStore(ObDASSearchCtx &search_ctx, ObDASScopeId scope = DAS_SCOPE_BASE)
    : RowIDStore(search_ctx, scope),
      store_(),
      rowids_(nullptr),
      capacity_(0),
      count_(0)
  {}
  ~CompactRowIDStore() override { reset(); }

  int init(int64_t capacity) override;
  int fill(const int64_t begin_idx_inclusive, const int64_t end_idx_exclusive, const common::ObIArray<ObExpr *> &rowid_exprs) override;
  void reset() override;
  void reuse() override;
  int64_t count() const override { return count_; }

  TO_STRING_KV(K_(count));

private:
  virtual int get_rowid(int64_t idx, ObDASRowID &rowid) const override;
  virtual int lower_bound(const ObDASRowID &target, const int64_t start_idx, int64_t &idx) const override;

private:
  ObTempRowStore store_;
  ObCompactRow **rowids_;
  int64_t capacity_;
  int64_t count_;
};

class ObIDASSearchOpParam
{
public:
  ObIDASSearchOpParam(ObDASSearchOpType op_type) : op_type_(op_type), is_scoring_(true) {}
  virtual ~ObIDASSearchOpParam() {}
  OB_INLINE ObDASSearchOpType get_op_type() const { return op_type_; }
  OB_INLINE void set_is_scoring(const bool is_scoring) { is_scoring_ = is_scoring; }
  OB_INLINE bool get_is_scoring() const { return is_scoring_; }
  virtual int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const = 0;
  VIRTUAL_TO_STRING_KV(K_(op_type), K_(is_scoring));
private:
  ObDASSearchOpType op_type_;
  bool is_scoring_;
};

class ObIDASSearchOp
{
public:
  ObIDASSearchOp(ObDASSearchCtx &search_ctx)
    : search_ctx_(search_ctx),
      op_type_(DAS_SEARCH_OP_MAX),
      op_id_(OB_INVALID_ID),
      scope_(search_ctx.get_op_creating_scope()),
      max_score_tuple_(),
      children_(nullptr),
      children_cnt_(0),
      min_competitive_score_(0.0),
      is_scoring_(true),
      profile_(nullptr)
  { }
  virtual ~ObIDASSearchOp() {}
  VIRTUAL_TO_STRING_KV(K_(op_id), K_(search_ctx));

  OB_INLINE void set_op_id(const int64_t id) { op_id_ = id; }
  OB_INLINE int64_t get_op_id() const { return op_id_; }
  OB_INLINE ObIDASSearchOp **get_children() { return children_; }
  OB_INLINE int64_t get_children_cnt() const { return children_cnt_; }
  OB_INLINE ObDASScopeId get_scope() const { return scope_; }
  OB_INLINE const common::ObIArray<ObExpr *> &get_rowid_exprs()
  { return *search_ctx_.get_scope(scope_).rowid_exprs_; }
  OB_INLINE const RowMeta &get_rowid_meta()
  { return search_ctx_.get_scope(scope_).rowid_meta_; }
  OB_INLINE int64_t max_batch_size() { return search_ctx_.max_batch_size(); }
  OB_INLINE ObEvalCtx &eval_ctx() { return *search_ctx_.eval_ctx_; }
  OB_INLINE sql::ObBitVector &mock_skip() { return *search_ctx_.mock_skip_; }
  OB_INLINE ObDASSearchOpType get_op_type() const { return op_type_; }
  int prepare_parallel_work_estimate();
  ObDASSearchWorkEstimate get_parallel_work_estimate() const
  {
    ObDASSearchWorkEstimate estimate;
    collect_parallel_work_estimate(estimate);
    return estimate;
  }
  OB_INLINE ObDASRowIDType get_rowid_type() const { return search_ctx_.get_scope(scope_).rowid_type_; }
  common::ObProfileId get_profile_id() const;
  int open_profile();
  void close_profile();
  void reset_profile();

  // RowID utility methods forwarded from search_ctx_, dispatching by scope_
  OB_INLINE int compare_rowid(const ObDASRowID &r1, const ObDASRowID &r2, int &cmp) const
  { return search_ctx_.compare_rowid(scope_, r1, r2, cmp); }
  OB_INLINE int deep_copy_rowid(const ObDASRowID &src, ObDASRowID &dst, common::ObIAllocator &alloc) const
  { return search_ctx_.deep_copy_rowid(scope_, src, dst, alloc); }
  OB_INLINE int create_rowid_store(int64_t capacity, RowIDStore *&store)
  { return search_ctx_.create_rowid_store(scope_, capacity, store); }
  OB_INLINE int rowid_to_expr(const ObDASRowID &rowid, int64_t batch_idx)
  { return search_ctx_.rowid_to_expr(scope_, rowid, batch_idx); }
  OB_INLINE int write_datum_to_rowid(const ObDatum &datum, ObDASRowID &rowid, common::ObIAllocator &alloc) const
  { return search_ctx_.write_datum_to_rowid(scope_, datum, rowid, alloc); }
  OB_INLINE int get_datum_from_rowid(const ObDASRowID &rowid, ObDatum &datum, int64_t col_idx = 0) const
  { return search_ctx_.get_datum_from_rowid(scope_, rowid, datum, col_idx); }

  int get_related_tablet_id(const ObDASScalarScanCtDef *scalar_ctdef, common::ObTabletID &tablet_id);
  static void switch_tablet_id(const share::ObLSID &new_ls_id, const common::ObTabletID &new_tablet_id, storage::ObTableScanParam &scan_param);

public:
// ---------------- core interfaces ----------------
  OB_INLINE int init(const ObIDASSearchOpParam &op_param);

  /*
   * Initializes the operator, opens children operators on demand, and advances to iterable state.
  */
  OB_INLINE int open();

  /*
   * Resets the operator to the state after open(), used in partition rescan.
  */
  OB_INLINE int rescan();

  /*
   * Switches the underlying scan to a different async-index segment (version range) without
   * changing the partition. Used by ObDASAsyncSegmentMergeOp to drive children across segments.
   * Distinct from rescan(), which means partition switch.
   * @in param range: The async-index segment version range to scan next.
  */
  OB_INLINE int rescan_segment(const common::ObNewVersionRange &range);

  /*
   * Closes the operator and its children, make the operator to the state before open().
  */
  OB_INLINE int close();

  /*
   * Advances to the next rowid that is greater than or equal to the target rowid.
   * @in param target: The target rowid to advance to.
   * @out param curr_id: The current rowid.
   * @out param score: The score of the current rowid.
   * Ensures returned curr_id lifetime is guaranteed until the next advance_to/next_rowid.
  */
  OB_INLINE int advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score);

  /*
   * Advances to the next rowid that is greater than or equal to the target rowid,
   * without materializing the score of the current rowid.
   * @in param target: The target rowid to advance to.
   * @out param curr_id: The current rowid.
   * The score must be loaded by load_curr_score() before it can be read.
   */
  OB_INLINE int advance_to_id(const ObDASRowID &target, ObDASRowID &curr_id);

  /*
   * Materializes the score of the current rowid on demand. It is valid after
   * advance_to_id() only when the caller confirms that curr_id is the target rowid.
   */
  OB_INLINE int load_curr_score(double &score);

  /*
   * Fetches the next rowid.
   * @out param next_id: The next rowid to be returned.
   * @out param score: The score of the next rowid.
   * Ensures returned next_id lifetime is guaranteed until the next advance_to/next_rowid.
  */
  OB_INLINE int next_rowid(ObDASRowID &next_id, double &score);

  /*
   * Advances shallowly using block max information, does not actually advance the rowid.
   * @in param target: The target rowid to advance to.
   * @in param inclusive: Whether target row is is inclusive in max score range,
   * @out param max_score_tuple: A possible MaxScoreTuple.
   * Ensures returned max_score_tuple lifetime is guaranteed until the next advance_to/next_rowid.
  */
  OB_INLINE int advance_shallow(const ObDASRowID &target, const bool inclusive, const MaxScoreTuple *&max_score_tuple);

  /*
   * Fetches the next batch of rowids.
   * @in param capacity: The capacity of the batch.
   * @out param count: The number of rowids fetched.
   * Unlike single-row interface, the batch interface does not support dynamic pruning, thus it directly
   * returns results via expressions instead of explicitly passing parameters.
   * Since most composite operators cannot truly be vectorized in complex query, the batch interface will
   * not be implemented for now.
   */
  OB_INLINE int next_rowids(int64_t capacity, int64_t &count);

  /*
   * Expr vectors populated by next_rowids(). Operators without a batch RowID
   * output contract return nullptr. Pass-through batch operators must override
   * this method instead of falling back to the scope anchor expressions.
   */
  virtual const common::ObIArray<ObExpr *> *get_rowid_output_exprs() const
  { return nullptr; }

  /*
   * Sets the minimum competitive score for dynamic pruning.
   * @in param threshold: The minimum competitive score.
   * Recursively pass the threshold to children operators by default.
   */
  OB_INLINE int set_min_competitive_score(const double &threshold);

  /*
   * Calculates the dimension-level max score, most operators do not need to implement.
   * @out param max_score: The dimension-level max score.
   */
  OB_INLINE int calc_max_score(double &threshold);
  // Return a conservative score bound for one forward rowid range without
  // advancing the row cursor. can_calc is false when global fallback is needed.
  OB_INLINE int calc_range_max_score(
      const ObDASRowID &start_id,
      const bool start_inclusive,
      const ObDASRowID &end_id,
      double &max_score,
      bool &can_calc);

  /*
   * Batch drains the op's next rows until boundary.
   *
   * Input:
   * - boundary: inclusive upper bound. Returned rowids must be <= boundary.
   * - capacity: max number of rows to write into out_ids/out_scores; must be positive.
   * - out_ids: output buffer for uint64 rowids; must not be null.
   * - out_scores: optional output buffer for scores. It can be null when scores are not needed.
   *
   * Output:
   * - count: number of rows written.
   * - next_id_after/next_score_after: meaningful only when count < capacity. In that case,
   *   next_id_after is the first prefetched row beyond boundary, or max if the op is exhausted.
   *
   * Semantics:
   * - Starts from the op's next row. Callers that cache a `current rowid` must handle it themselves
   *   before calling this method.
   * - If count == capacity, the output buffer is full; callers may invoke this method again with
   *   the same boundary to continue draining.
   * - This public wrapper does not return OB_ITER_END. EOF is represented as:
   *       ret = OB_SUCCESS with count < capacity and next_id_after set to max.
   * - The protected do_drain_until() pulls rows according to the same next-row contract. Its
   *   base implementation falls back to do_next_rowid() row by row; derived ops may override it
   *   with a real batch implementation.
   */
  OB_INLINE int drain_until(
      const ObDASRowID &boundary,
      int64_t capacity,
      uint64_t *out_ids,
      double *out_scores,
      int64_t &count,
      ObDASRowID &next_id_after,
      double &next_score_after);
  // Position at the accumulator window start, consume every row through boundary,
  // and add scores directly to candidates that are still active.
  int drain_until_active(
      const ObDASRowID &boundary,
      DocScoreAccumulator &accumulator);
  /*
   * Drains every matching uint64 RowID in inclusive [range_start, range_end]
   * into a caller-owned consumer. Each consumer callback receives a nondecreasing
   * batch. Across callbacks, neither global ordering nor uniqueness is guaranteed.
   * next_id_after is the smallest match greater than range_end, or max at EOF.
   * Like the ordered iterator APIs, this is stateful and forward-only; successive
   * ranges must not move backward. EOF is returned as OB_SUCCESS with
   * next_id_after set to max. After an error other than OB_ITER_END, callers must
   * rescan or destroy the operator before invoking it again.
   */
  virtual bool supports_direct_rowid_drain() const { return false; }
  OB_INLINE int direct_drain_rowids(
      const uint64_t range_start,
      const uint64_t range_end,
      ObDASRowIDBatchConsumer &consumer,
      ObDASRowID &next_id_after)
  {
    int ret = OB_SUCCESS;
    next_id_after.set_max();
    if (OB_UNLIKELY(range_start > range_end)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_LIKELY(nullptr == profile_)) {
      ret = do_direct_drain_rowids(
          range_start, range_end, consumer, next_id_after);
    } else {
      common::ObProfileSwitcher switcher(profile_);
      common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
      common::ScopedTimer timer(common::ObMetricId::HS_NEXT_ROWIDS_TIME);
      INC_METRIC_VAL(common::ObMetricId::HS_NEXT_ROWIDS_COUNT, 1);
      ret = do_direct_drain_rowids(
          range_start, range_end, consumer, next_id_after);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to direct drain rowids", KR(ret), K(range_start),
          K(range_end), K(next_id_after));
    }
    return ret;
  }
  /*
   * Probes whether the given target rowid matches this operator's filter set.
   * Only the non-driver primary ror op of conjunction supports probe mode currently.
   */
  virtual bool is_probe_mode() const { return false; }
  // Whether advance_to_id() and load_curr_score() are implemented by this operator.
  virtual bool can_advance_to_id() const { return false; }
  /*
   * Whether next_rowid()/drain_until() can enumerate this operator's complete
   * match set without a caller-provided candidate rowid. By default, an operator
   * is enumerable only when both itself and all of its children are enumerable.
   * Composite operators whose output is driven by only a subset of their children
   * must override this method to describe that dependency.
   */
  virtual bool can_enumerate_rowids() const
  {
    bool can_enumerate = !is_probe_mode();
    for (int64_t i = 0; can_enumerate && i < children_cnt_; ++i) {
      can_enumerate = OB_NOT_NULL(children_[i])
          && children_[i]->can_enumerate_rowids();
    }
    return can_enumerate;
  }
  OB_INLINE int probe(const ObDASRowID &target, bool &hit);
// ---------------- core interfaces end ----------------

protected:
// ---------------- interfaces implementation ----------------
  virtual int do_init(const ObIDASSearchOpParam &op_param) { return OB_SUCCESS; }
  virtual int do_open() = 0;
  virtual int do_close() = 0;
  virtual int do_rescan() = 0;
  // Default: propagate rescan_segment to all children. Async leaves override to reopen posting iter.
  virtual int do_rescan_segment(const common::ObNewVersionRange &range);
  virtual int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) = 0;
  virtual int do_advance_to_id(const ObDASRowID &target, ObDASRowID &curr_id);
  virtual int do_next_rowid(ObDASRowID &next_id, double &score) = 0;
  virtual int do_load_curr_score(double &score);
  virtual int do_advance_shallow(const ObDASRowID &target, const bool inclusive, const MaxScoreTuple *&max_score_tuple);
  virtual int do_next_rowids(int64_t capacity, int64_t &count);
  virtual int do_set_min_competitive_score(const double &threshold);
  virtual int do_calc_max_score(double &threshold);
  virtual int do_calc_range_max_score(
      const ObDASRowID &start_id,
      const bool start_inclusive,
      const ObDASRowID &end_id,
      double &max_score,
      bool &can_calc)
  {
    UNUSED(start_id);
    UNUSED(start_inclusive);
    UNUSED(end_id);
    UNUSED(max_score);
    can_calc = false;
    return OB_SUCCESS;
  }
  virtual int do_prepare_parallel_work_estimate() { return OB_SUCCESS; }
  virtual int64_t get_self_estimated_work_row_count() const { return -1; }
  virtual int do_drain_until(
      const ObDASRowID &boundary,
      int64_t capacity,
      uint64_t *out_ids,
      double *out_scores,
      int64_t &count,
      ObDASRowID &next_id_after,
      double &next_score_after);
  virtual int do_drain_until_active(
      const ObDASRowID &boundary,
      DocScoreAccumulator &accumulator,
      int64_t &count);
  virtual int do_direct_drain_rowids(
      const uint64_t range_start,
      const uint64_t range_end,
      ObDASRowIDBatchConsumer &consumer,
      ObDASRowID &next_id_after)
  {
    int ret = OB_NOT_SUPPORTED;
    UNUSEDx(range_start, range_end, consumer, next_id_after);
    return ret;
  }
  virtual int do_probe(const ObDASRowID &target, bool &hit) { return OB_NOT_SUPPORTED; }
// ---------------- interfaces implementation end ----------------

  OB_INLINE common::ObIAllocator &ctx_allocator() const { return search_ctx_.get_allocator(); }

private:
  OB_INLINE int drain_until_impl(
      const ObDASRowID &boundary,
      int64_t capacity,
      uint64_t *out_ids,
      double *out_scores,
      int64_t &count,
      ObDASRowID &next_id_after,
      double &next_score_after);
  bool is_parallel_ror_op() const
  {
    // 异步 Scalar ROR 尚未提供不重复计数的工作量估算，本期不参与 AUTO DOP 准入。
    return DAS_SEARCH_OP_TOKEN == op_type_
        || DAS_SEARCH_OP_SCALAR_INDEX_ROR == op_type_
        || DAS_SEARCH_OP_SCALAR_PRIMARY_ROR == op_type_
        || DAS_SEARCH_OP_ASYNC_TOKEN == op_type_;
  }
  void collect_parallel_work_estimate(
      ObDASSearchWorkEstimate &estimate) const
  {
    if (DAS_SEARCH_OP_BITMAP == op_type_ || is_probe_mode()) {
      // Bitmap PREPARE 和 probe-mode 算子不是可按 Range 切分的 ROR 工作，
      // 因此它们及其实现子树都不参与简化的 AUTO DOP 准入规则。
    } else if (is_parallel_ror_op()) {
      estimate.add_ror_work(get_self_estimated_work_row_count());
    } else if (DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE == op_type_) {
      // 每个 segment child 都是同一棵逻辑查询树的副本。异步 token 的 posting
      // 估算覆盖整个索引 tablet，因此只统计一棵代表树，避免按 segment 数重复放大。
      if (children_cnt_ > 0 && OB_NOT_NULL(children_[0])) {
        children_[0]->collect_parallel_work_estimate(estimate);
      }
    } else {
      for (int64_t i = 0; i < children_cnt_; ++i) {
        if (OB_NOT_NULL(children_[i])) {
          children_[i]->collect_parallel_work_estimate(estimate);
        }
      }
    }
  }
  int init_children_ops_array(const ObIArray<ObIDASSearchOp *> &children);

protected:
  ObDASSearchCtx &search_ctx_;
  ObDASSearchOpType op_type_;
  int64_t op_id_;
  ObDASScopeId scope_;
  MaxScoreTuple max_score_tuple_;
  ObIDASSearchOp **children_;
  int64_t children_cnt_;
  double min_competitive_score_;
  bool is_scoring_;
  common::ObOpProfile<common::ObMetric> *profile_;
};

OB_INLINE int ObIDASSearchOp::init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObIDASSearchOp *> children;
  if (OB_FAIL(do_init(op_param))) {
    LOG_WARN("failed to init impl", K(ret));
  } else if (OB_FAIL(op_param.get_children_ops(children))) {
    LOG_WARN("failed to get children ops", KR(ret));
  } else if (OB_FAIL(init_children_ops_array(children))) {
    LOG_WARN("failed to init children ops array", KR(ret));
  } else {
    op_type_ = op_param.get_op_type();
    is_scoring_ = op_param.get_is_scoring();
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(open_profile())) {
    LOG_WARN("failed to open profile", KR(ret));
  } else {
    common::ObProfileSwitcher switcher(profile_);
    if (OB_FAIL(do_open())) {
      LOG_WARN("failed to open impl", KR(ret));
    }
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_close())) {
    LOG_WARN("failed to close impl", KR(ret));
  }
  op_type_ = DAS_SEARCH_OP_MAX;
  op_id_ = OB_INVALID_ID;
  max_score_tuple_.reset();
  min_competitive_score_ = 0.0;
  if (OB_NOT_NULL(children_)) {
    ctx_allocator().free(children_);
    children_ = nullptr;
  }
  children_cnt_ = 0;
  close_profile();
  return ret;
}

OB_INLINE int ObIDASSearchOp::rescan()
{
  int ret = OB_SUCCESS;
  // for vector index search, in post-iterative filter mode, we can reuse the old profile in the same tablet execution
  // otherwise, every iteration will create a new profile, which will overwhelm the profile output
  bool is_vec_index_search = search_ctx_.is_vec_index_search();
  if (!is_vec_index_search && OB_FAIL(open_profile())) {
    LOG_WARN("failed to open profile", KR(ret));
  } else {
    common::ObProfileSwitcher switcher(profile_);
    if (is_vec_index_search) {
      // only if in the same tablet execution, we can reuse the old profile and record on it
      INC_METRIC_VAL(common::ObMetricId::HS_SAME_OP_SCAN_TIMES, 1);
    }
    min_competitive_score_ = 0.0;
    if (OB_FAIL(do_rescan())) {
      LOG_WARN("failed to rescan impl", KR(ret));
    }
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::rescan_segment(const common::ObNewVersionRange &range)
{
  int ret = OB_SUCCESS;
  common::ObProfileSwitcher switcher(profile_);
  min_competitive_score_ = 0.0;
  if (OB_FAIL(do_rescan_segment(range))) {
    LOG_WARN("failed to rescan segment impl", KR(ret));
  }
  return ret;
}

// profile_ is null when runtime diagnosis is off. Skip switcher/timers/metric
// bumps on that path: advance_to/advance_to_id/next_rowid and score loading are per-row hot loops.
OB_INLINE int ObIDASSearchOp::advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr == profile_)) {
    ret = do_advance_to(target, curr_id, score);
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_ADVANCE_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_ADVANCE_COUNT, 1);
    ret = do_advance_to(target, curr_id, score);
    INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, 1);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to impl", KR(ret));
  } else if (!is_scoring_) {
    score = 0.0;
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::advance_to_id(const ObDASRowID &target, ObDASRowID &curr_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!can_advance_to_id())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("advance to id is not supported", K(ret), K(target));
  } else {
    if (OB_LIKELY(nullptr == profile_)) {
      ret = do_advance_to_id(target, curr_id);
    } else {
      common::ObProfileSwitcher switcher(profile_);
      common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
      common::ScopedTimer timer(common::ObMetricId::HS_ADVANCE_TIME);
      INC_METRIC_VAL(common::ObMetricId::HS_ADVANCE_COUNT, 1);
      ret = do_advance_to_id(target, curr_id);
      INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, 1);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to id impl", KR(ret), K(target));
    }
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::load_curr_score(double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (OB_UNLIKELY(!can_advance_to_id())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("advance to id is not supported", K(ret));
  } else if (!is_scoring_) {
    // Non-scoring operators do not need to load a score.
  } else {
    if (OB_LIKELY(nullptr == profile_)) {
      ret = do_load_curr_score(score);
    } else {
      common::ObProfileSwitcher switcher(profile_);
      common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
      common::ScopedTimer timer(common::ObMetricId::HS_ADVANCE_TIME);
      ret = do_load_curr_score(score);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to load curr score", K(ret));
    }
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr == profile_)) {
    ret = do_next_rowid(next_id, score);
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_NEXT_ROWID_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_NEXT_ROWID_COUNT, 1);
    ret = do_next_rowid(next_id, score);
    INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, 1);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to next rowid impl", KR(ret));
  } else if (!is_scoring_) {
    score = 0.0;
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::advance_shallow(const ObDASRowID &target, const bool inclusive, const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr == profile_)) {
    ret = do_advance_shallow(target, inclusive, max_score_tuple);
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_ADVANCE_SHALLOW_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_ADVANCE_SHALLOW_COUNT, 1);
    ret = do_advance_shallow(target, inclusive, max_score_tuple);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance shallow impl", KR(ret));
  } else if (!is_scoring_ && nullptr != max_score_tuple) {
    max_score_tuple_.set(max_score_tuple->get_min_id(), max_score_tuple->get_max_id(), 0.0);
    max_score_tuple = &max_score_tuple_;
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::next_rowids(int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr == profile_)) {
    ret = do_next_rowids(capacity, count);
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_NEXT_ROWIDS_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_NEXT_ROWIDS_COUNT, 1);
    ret = do_next_rowids(capacity, count);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to next rowids impl", KR(ret));
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::set_min_competitive_score(const double &threshold)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr == profile_)) {
    ret = do_set_min_competitive_score(threshold);
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_SET_MIN_COMPETITIVE_SCORE_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_SET_MIN_COMPETITIVE_SCORE_COUNT, 1);
    ret = do_set_min_competitive_score(threshold);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to set min competitive score impl", KR(ret));
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr == profile_)) {
    ret = do_calc_max_score(threshold);
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_CALC_MAX_SCORE_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_CALC_MAX_SCORE_COUNT, 1);
    ret = do_calc_max_score(threshold);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to calc max score impl", KR(ret));
  } else if (!is_scoring_) {
    threshold = 0.0;
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::calc_range_max_score(
    const ObDASRowID &start_id,
    const bool start_inclusive,
    const ObDASRowID &end_id,
    double &max_score,
    bool &can_calc)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr == profile_)) {
    ret = do_calc_range_max_score(start_id, start_inclusive, end_id, max_score, can_calc);
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_CALC_RANGE_MAX_SCORE_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_CALC_RANGE_MAX_SCORE_COUNT, 1);
    ret = do_calc_range_max_score(start_id, start_inclusive, end_id, max_score, can_calc);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to calc range max score impl", KR(ret),
        K(start_id), K(start_inclusive), K(end_id));
  } else if (OB_SUCC(ret) && can_calc && !is_scoring_) {
    max_score = 0.0;
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::drain_until(
    const ObDASRowID &boundary,
    int64_t capacity,
    uint64_t *out_ids,
    double *out_scores,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  int ret = OB_SUCCESS;
  count = 0;
  next_id_after.reset();
  next_score_after = 0.0;
  if (OB_LIKELY(nullptr == profile_)) {
    if (OB_FAIL(drain_until_impl(
        boundary, capacity, out_ids, out_scores, count, next_id_after, next_score_after))) {
      LOG_WARN("failed to drain_until impl", KR(ret));
    }
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_NEXT_ROWIDS_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_NEXT_ROWIDS_COUNT, 1);
    if (OB_FAIL(drain_until_impl(
        boundary, capacity, out_ids, out_scores, count, next_id_after, next_score_after))) {
      LOG_WARN("failed to drain_until impl", KR(ret));
    }
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::drain_until_impl(
    const ObDASRowID &boundary,
    int64_t capacity,
    uint64_t *out_ids,
    double *out_scores,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity <= 0 || OB_ISNULL(out_ids) || boundary.is_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid drain_until arguments", K(ret), K(capacity), KP(out_ids), K(boundary));
  } else if (OB_UNLIKELY(DAS_ROWID_TYPE_UINT64 != get_rowid_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("drain_until only supports uint64 rowid type", K(ret), K(get_rowid_type()));
  } else if (OB_FAIL(do_drain_until(
      boundary, capacity, out_ids, out_scores, count, next_id_after, next_score_after))) {
    LOG_WARN("failed to drain_until impl", KR(ret));
  } else if (OB_UNLIKELY(count < 0 || count > capacity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected drain_until output count", K(ret), K(count), K(capacity));
  } else {
    if (OB_NOT_NULL(profile_)) {
      const int64_t output_count = count + (count < capacity && !next_id_after.is_max() ? 1 : 0);
      INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, output_count);
    }
    if (!is_scoring_) {
      next_score_after = 0.0;
      if (nullptr != out_scores && count > 0) {
        MEMSET(out_scores, 0, count * sizeof(double));
      }
    }
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::probe(const ObDASRowID &target, bool &hit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_probe_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this op does not support probe", KR(ret), K(op_type_));
  } else if (OB_LIKELY(nullptr == profile_)) {
    if (OB_FAIL(do_probe(target, hit))) {
      LOG_WARN("failed to probe impl", KR(ret), K(target));
    }
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_PROBE_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_PROBE_COUNT, 1);
    if (OB_FAIL(do_probe(target, hit))) {
      LOG_WARN("failed to probe impl", KR(ret), K(target));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_I_DAS_SEARCH_OP_H_
