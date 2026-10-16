/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_BITMAP_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_BITMAP_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_scalar_scan_op.h"
#include "sql/das/search/ob_das_filter_bitmap_provider.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"

namespace oceanbase
{
namespace sql
{

class ObFastBitmap
{
 public:
  static const uint64_t CHUNK_SHIFT = 16;
  static const uint64_t CHUNK_SIZE = 1 << CHUNK_SHIFT;
  static const uint64_t CHUNK_MASK = CHUNK_SIZE - 1;
  static const uint64_t WORD_SHIFT = 6;
  static const uint64_t WORD_BITS = 64;
  static const uint64_t WORDS_PER_CHUNK = CHUNK_SIZE / WORD_BITS;
  static const int64_t MAX_BITMAP_RESERVED_MEM_SIZE = 10 * 1024 * 1024; // 10MB

  class ChunkInfo
  {
   public:
    ChunkInfo() : min_val_(UINT16_MAX), max_val_(0), chunk_() {}
    ~ChunkInfo() {}

    TO_STRING_KV(K_(chunk), K_(min_val), K_(max_val));

    uint16_t min_val_;
    uint16_t max_val_;
    uint64_t chunk_[WORDS_PER_CHUNK];
  };

  class Iterator
  {
   public:
    enum DrainState
    {
      DRAIN_BATCH_FULL,
      DRAIN_BOUNDARY_REACHED,
      DRAIN_ITER_END
    };

    Iterator()
      : bm_(nullptr),
        cur_val_(-1)
    { }
    OB_INLINE bool is_valid() const { return OB_NOT_NULL(bm_) && !bm_->stats_dirty_; }
    int init(const ObFastBitmap *bm);
    void reuse();
    void reset();
    bool is_inited() const { return bm_ != nullptr; }
    int next_id(uint64_t &val);
    int advance_to(const uint64_t target, uint64_t &val);
    // Decode set bits directly from bitmap words, starting strictly after
    // cur_val_. The boundary is inclusive.
    //
    // DRAIN_BATCH_FULL: count == capacity; cursor is on the last output row.
    // DRAIN_BOUNDARY_REACHED: cursor/next_id_after is the first row > boundary.
    // DRAIN_ITER_END: no more bitmap rows; count may be smaller than capacity.
    int drain_until(
        const uint64_t boundary,
        const int64_t capacity,
        uint64_t *out_ids,
        int64_t &count,
        uint64_t &next_id_after,
        DrainState &state);

   private:
    const ObFastBitmap *bm_;
    uint64_t cur_val_;
  };

  friend class Iterator;

  ObFastBitmap(common::ObIAllocator &allocator)
    : cardinality_(0),
      stats_dirty_(false),
      chunks_(),
      allocator_(allocator),
      info_allocator_(common::ObMemAttr(MTL_ID(), "DASFastBitmap")),
      chunk_buf_(nullptr),
      chunk_buf_count_(0),
      chunk_buf_idx_(0)
  { }

  ~ObFastBitmap();

  void reset();
  void reuse();
  int64_t cardinality() const
  {
    OB_ASSERT(!stats_dirty_);
    return cardinality_;
  }
  int init(int64_t max_value_count);
  int add(uint64_t val);
  // add_batch() only ensures chunks and sets bits. Call finalize() before
  // reading statistics or iterating over the bitmap.
  int add_batch(const uint64_t *vals, const int64_t count);
  void finalize();
  // Copy the bitmap slice [range_begin, range_end] into a dense destination
  // window whose bit zero represents window_begin. The destination is reset
  // before copying, and copied_count returns the number of set bits copied.
  int copy_range_to_words(
      const uint64_t range_begin,
      const uint64_t range_end,
      const uint64_t window_begin,
      const int64_t window_capacity,
      uint64_t *dst_words,
      int64_t &copied_count) const;

 private:
  int alloc_chunk(uint64_t chunk_idx, ChunkInfo *&chunk_info);
  uint64_t get_word(const uint64_t global_word_idx) const;

 private:
  int64_t cardinality_;
  bool stats_dirty_;
  ObSEArray<ChunkInfo*, 32> chunks_;
  common::ObIAllocator &allocator_;
  common::ObArenaAllocator info_allocator_;
  ChunkInfo *chunk_buf_;
  int64_t chunk_buf_count_;
  int64_t chunk_buf_idx_;
};

// Synchronously transfers producer-owned RowID views into an idempotent bitmap.
// The producer may invalidate its view as soon as consume() returns.
class ObFastBitmapRowIDConsumer : public ObDASRowIDBatchConsumer
{
public:
  explicit ObFastBitmapRowIDConsumer(ObFastBitmap &bitmap)
    : bitmap_(bitmap), consumed_count_(0)
  {}
  ~ObFastBitmapRowIDConsumer() override {}

  int consume(const uint64_t *rowids, const int64_t count) override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(count < 0 || (count > 0 && OB_ISNULL(rowids)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid rowid batch", K(ret), KP(rowids), K(count));
    } else if (count > 0 && OB_FAIL(bitmap_.add_batch(rowids, count))) {
      LOG_WARN("failed to add rowid batch to bitmap", K(ret), K(count));
    } else {
      consumed_count_ += count;
    }
    return ret;
  }

  int64_t consumed_count() const { return consumed_count_; }

private:
  ObFastBitmap &bitmap_;
  int64_t consumed_count_;
};
 
class ObDASBitmapOpParam : public ObIDASSearchOpParam
{
public:
  ObDASBitmapOpParam(ObIDASSearchOp *child,
                     const ObDASScalarScanCtDef *scalar_ctdef,
                     ObDASScalarScanRtDef *scalar_rtdef)
  : ObIDASSearchOpParam(DAS_SEARCH_OP_BITMAP),
    child_(child),
    scalar_ctdef_(scalar_ctdef),
    scalar_rtdef_(scalar_rtdef),
    provider_(nullptr)
  { }
  ObDASBitmapOpParam(ObDASFilterBitmapProvider *provider)
  : ObIDASSearchOpParam(DAS_SEARCH_OP_BITMAP),
    child_(nullptr),
    scalar_ctdef_(nullptr),
    scalar_rtdef_(nullptr),
    provider_(provider)
  { }
  virtual ~ObDASBitmapOpParam() {}
  OB_INLINE ObIDASSearchOp *get_child() const { return child_; }
  OB_INLINE const ObDASScalarScanCtDef *get_scalar_ctdef() const { return scalar_ctdef_; }
  OB_INLINE ObDASScalarScanRtDef *get_scalar_rtdef() const { return scalar_rtdef_; }
  OB_INLINE ObDASFilterBitmapProvider *get_provider() const { return provider_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
private:
  ObIDASSearchOp *child_;
  const ObDASScalarScanCtDef *scalar_ctdef_;
  ObDASScalarScanRtDef *scalar_rtdef_;
  ObDASFilterBitmapProvider *provider_;
};

class ObDASBitmapOp : public ObIDASSearchOp
{
public:
  ObDASBitmapOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    exhausted_(true),
    bitmap_built_(false),
    owns_bitmap_(false),
    has_pending_first_(false),
    pending_first_val_(0),
    bitmap_(nullptr),
    bitmap_iter_(),
    rowid_expr_(nullptr),
    input_op_(nullptr),
    provider_(nullptr),
    boost_weight_(0.0)
  { }
  virtual ~ObDASBitmapOp() {}

  // Must be called before do_open(). Caller retains ownership of bm.
  int set_external_bitmap(ObFastBitmap *bm);
  int build_shared_bitmap(common::ObIAllocator &bitmap_alloc, ObFastBitmap *&out_bitmap);
  // Fill the final provider bitmap only from an exclusive, unread range source.
  int try_fill_provider_bitmap(ObFastBitmap &bitmap, bool &used_direct_build);
  OB_INLINE bool is_filter_bitmap_view() const { return OB_NOT_NULL(provider_); }
  // Fast path for BMM filter windows. It copies provider bitmap words directly
  // into the BMM-local bitset and advances curr_id to the first hit after
  // batch_end_id, avoiding Bitmap -> RowID -> Bitmap conversion.
  int drain_into_filter_bitset(
      const ObDASRowID &batch_start_id,
      const ObDASRowID &batch_end_id,
      const int64_t capacity,
      uint64_t *filter_words,
      ObDASRowID &curr_id,
      bool &iter_end);

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_rescan_segment(const common::ObNewVersionRange &range) override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_drain_until(
      const ObDASRowID &boundary,
      int64_t capacity,
      uint64_t *out_ids,
      double *out_scores,
      int64_t &count,
      ObDASRowID &next_id_after,
      double &next_score_after) override;
  int do_advance_shallow(const ObDASRowID &target, const bool inclusive, const MaxScoreTuple *&max_score_tuple) override;
  int do_calc_max_score(double &threshold) override;

private:
  int build_bitmap();
  int ensure_bitmap_ready();
  int init_provider_bitmap_view();
  int do_drain_into_filter_bitset(
      const ObDASRowID &batch_start_id,
      const ObDASRowID &batch_end_id,
      const int64_t capacity,
      uint64_t *filter_words,
      ObDASRowID &curr_id,
      bool &iter_end,
      int64_t &observed_count);
  int fill_bitmap_from_scan(ObFastBitmap &bitmap);
  // Position bitmap_iter_ at the first val >= docid_range_lo and buffer it into
  // pending_first_val_. Must be called after bitmap_built_/exhausted_ are set.
  int seek_iter_to_lo();
  OB_INLINE bool is_beyond_docid_range_hi(uint64_t val) const
  {
    return search_ctx_.has_docid_range()
        && !search_ctx_.get_docid_range_hi().is_max_value()
        && val > search_ctx_.get_docid_range_hi().get_uint64();
  }

private:
  bool exhausted_;
  bool bitmap_built_;
  // true iff this op owns a self-built bitmap. External bitmap injection and
  // Provider Views borrow bitmap_ from the caller/provider and only reset
  // their own iterator; they never clear or free the shared bitmap.
  bool owns_bitmap_;
  // Buffered result of the initial advance_to(docid_range_lo). Consumed by the
  // first do_next_rowid() or a do_advance_to() whose target <= pending_first_val_.
  bool has_pending_first_;
  uint64_t pending_first_val_;
  ObFastBitmap *bitmap_;
  ObFastBitmap::Iterator bitmap_iter_;
  ObExpr *rowid_expr_;
  ObIDASSearchOp *input_op_;
  ObDASFilterBitmapProvider *provider_;
  double boost_weight_;
};

// Named type for the provider/view ownership boundary. It deliberately keeps
// DAS_SEARCH_OP_BITMAP so BMM and XProfile remain backward compatible.
class ObDASFilterBitmapViewOp : public ObDASBitmapOp
{
public:
  explicit ObDASFilterBitmapViewOp(ObDASSearchCtx &search_ctx)
    : ObDASBitmapOp(search_ctx)
  { }
  virtual ~ObDASFilterBitmapViewOp() {}
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_BITMAP_OP_H_
