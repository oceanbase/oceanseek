/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_SQL_DAS_SORT_TOPN_ITER_H_
#define OCEANBASE_SQL_DAS_SORT_TOPN_ITER_H_
#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_local_lookup_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/engine/sort/ob_sort_op_impl.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_search_define.h"
#include "lib/container/ob_heap.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/rc/context.h"
#include "share/diagnosis/ob_runtime_profile.h"

namespace oceanbase
{
namespace sql
{
class ObDASSearchDriverIter;

/**
 * @brief Keep at most capacity rows for hybrid sort, with the last row in sort order at the heap root.
 * Deduplicate by __shard_doc; repeated rows must have the same sort values and raw score.
 */
class ObDASSortTopNHeap
{
public:
  /** @brief Create an empty heap; its pointer-array allocator must outlive the heap. */
  explicit ObDASSortTopNHeap(common::ObIAllocator &heap_allocator)
    : sort_ctdef_(nullptr),
      allocator_(nullptr),
      capacity_(0),
      cmp_(),
      heap_(cmp_, &heap_allocator)
  {}
  /** @brief Release retained entries and the __shard_doc-to-entry map. */
  ~ObDASSortTopNHeap() { reset(); }
  /**
   * @brief Set the sort rules, capacity, and retained-row allocator; sort must include __shard_doc.
   * Capacity must be positive.
   * sort_ctdef and allocator must remain valid until reset().
   */
  int init(const ObDASHybridSearchSortCtDef &sort_ctdef, int64_t capacity, common::ObIAllocator &allocator);
  /**
   * @brief Add a row, or replace the heap root if full and the new row sorts before it.
   * Retained rows are deep-copied and replaced rows are freed.
   * Skip duplicate __shard_doc values, but return an error if their sort values or raw scores differ.
   */
  int offer(const ObDASSortTopNEntry &entry);
  /**
   * @brief Empty the heap into rows in sort order, deep-copying values into allocator.
   * The caller must first free any values already in rows; partial output may remain on error.
   */
  int drain_sorted(common::ObIArray<ObDASSortTopNEntry> &rows, common::ObIAllocator &allocator);
  /** @brief Free retained rows and clear state for another init(). */
  void reset();
  /** @brief Whether the heap is full, allowing the search driver to skip rows that cannot enter TopN. */
  bool full() const { return capacity_ > 0 && heap_.count() == capacity_; }
  /** @brief Return the last retained row in sort order; the heap must be nonempty. */
  const ObDASSortTopNEntry &top() const { return *heap_.top(); }
private:
  /** @brief Compare sort values so the last row in sort order stays at the heap root. */
  struct Compare
  {
    Compare() : sort_ctdef_(nullptr), ret_(common::OB_SUCCESS) {}
    bool operator()(const ObDASSortTopNEntry *left, const ObDASSortTopNEntry *right);
    int get_error_code() const { return ret_; }
    void reset()
    {
      sort_ctdef_ = nullptr;
      ret_ = common::OB_SUCCESS;
    }
    const ObDASHybridSearchSortCtDef *sort_ctdef_;
    int ret_; // First comparison error, checked by heap operations and cleared by reset().
  };
  /** @brief Free a retained row and its sort values. */
  void free_entry(ObDASSortTopNEntry *entry);
  const ObDASHybridSearchSortCtDef *sort_ctdef_; // Borrowed sort rules and __shard_doc position.
  common::ObIAllocator *allocator_; // Allocates retained rows and their copied sort values.
  int64_t capacity_; // Maximum rows retained for the requested size and offset.
  Compare cmp_;
  common::ObBinaryHeap<ObDASSortTopNEntry *, Compare, 16> heap_;
  // Map __shard_doc (32-character hex string) -> the document's entry in heap_.
  common::hash::ObHashMap<common::ObString, ObDASSortTopNEntry *> shard_doc_entries_;
};

/**
 * @brief A row from the search driver, before fetching its sort fields from the main table.
 */
struct ObDASSortCandidate
{
  uint64_t rowid_ = 0; // HEAP __pk_increment used to look up the main-table row.
  double raw_score_ = 0; // Search score saved before the lookup.
  common::ObDatum shard_doc_value_; // __shard_doc bytes owned by the candidate batch allocator.
  TO_STRING_KV(K_(rowid), K_(raw_score), K_(shard_doc_value));
};

/**
 * @brief A finite candidate batch feeding the existing local-lookup state machine.
 * It never reads the driver: exhausting this input must not open the next Group.
 */
class ObDASSortCandidateIter : public ObDASIter
{
public:
  ObDASSortCandidateIter() : ObDASIter(), candidates_(nullptr), rowkey_(nullptr), pos_(0) {}
  void bind(const common::ObIArray<ObDASSortCandidate> &rows, ObExpr &rowkey)
  {
    candidates_ = &rows;
    rowkey_ = &rowkey;
    pos_ = 0;
  }
protected:
  int inner_init(ObDASIterParam &param) override
  {
    UNUSED(param);
    return OB_SUCCESS;
  }
  int inner_reuse() override
  {
    pos_ = 0;
    return OB_SUCCESS;
  }
  int inner_release() override
  {
    candidates_ = nullptr;
    rowkey_ = nullptr;
    pos_ = 0;
    return OB_SUCCESS;
  }
  int inner_get_next_row() override { return OB_NOT_SUPPORTED; }
  int inner_get_next_rows(int64_t &count, int64_t capacity) override;
private:
  const common::ObIArray<ObDASSortCandidate> *candidates_;
  ObExpr *rowkey_;
  int64_t pos_;
};

/**
 * @brief Reuse LocalLookup for sort columns; retain identities for one driver batch.
 * Candidate/hash storage is independent of lookup_memctx_, which the parent
 * resets between its internal point-get requests. No additional profile node.
 */
class ObDASSortValueLookupIter : public ObDASLocalLookupIter
{
public:
  ObDASSortValueLookupIter();
  ~ObDASSortValueLookupIter();
  int init_lookup(ObDASSearchCtx &ctx);
  int prepare_execution();
  int begin_candidate_batch(ObDASSearchDriverIter &driver, int64_t count);
  int restore_candidate_batch(int64_t count);
  ObEvalCtx &get_value_eval_ctx() const { return *private_eval_ctx_; }
  bool batch_finished() const { return batch_finished_; }
  // Close storage/hash state. Private frames remain valid until this object's destructor.
  int destroy_lookup();
protected:
  int inner_get_next_row() override { return OB_NOT_SUPPORTED; }
  int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  int do_index_lookup() override;
  int check_index_lookup() override;
private:
  int init_private_frames();
  int refresh_parameter_frames();
  int prepare_batch_binding();
  ObDASSearchCtx *search_ctx_;
  const ObDASHybridSearchSortCtDef *sort_ctdef_;
  // Lookup lifetime: private frames, copied RtDef and reusable array buffers.
  common::ObArenaAllocator allocator_;
  // Candidate-batch lifetime: copied shard_doc payloads, replaced after lookup finishes the batch.
  common::ObArenaAllocator candidate_allocator_;
  ObEvalCtx *private_eval_ctx_;
  ObDASScanRtDef rtdef_;
  ObDASSortCandidateIter candidate_iter_;
  ObDASScanIter scan_iter_;
  ObDASIter *lookup_children_[2];
  common::ObSEArray<ObDASSortCandidate, 64, common::ObIAllocator &> candidates_;
  common::hash::ObHashMap<uint64_t, int64_t> candidate_indices_;
  common::ObSEArray<int64_t, 64, common::ObIAllocator &> returned_indices_;
  bool batch_finished_;
};

/**
 * @brief Add only cursor/bottom access to the existing sort implementation.
 * Batch input, sorting, batch output and memory/dump policy are inherited,
 * not reimplemented. Stored rows contain sort keys followed by required payload.
 */
class ObDASHybridTopNImpl : public ObSortOpImpl
{
public:
  ObDASHybridTopNImpl();
  ~ObDASHybridTopNImpl() override { reset(); }
  int init_hybrid_sort(const ObDASHybridSearchSortCtDef &ctdef, ObDASSearchCtx &ctx, ObEvalCtx &eval);
  int prepare_heap_sort(const ObDASHybridSearchSortCtDef &ctdef, int64_t capacity, ObEvalCtx &eval);
  int is_after(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval, bool &passes);
  const common::ObDatum *get_bottom() const;
  /** @brief Clear one execution's rows and status, preserving comparison rules and the cursor. */
  void reuse() override;
  /** @brief Clear the cursor and sort configuration; the base destructor releases the context. */
  void reset() override;
private:
  Compare after_cmp_;
  ObChunkDatumStore::StoredRow *after_row_;
};

/**
 * @brief Parameters for the hybrid sort TopN iterator.
 */
struct ObDASSortTopNIterParam : ObDASIterParam
{
  /** @brief Set sort parameters; the caller must also set common DAS iterator parameters. */
  ObDASSortTopNIterParam(const ObDASHybridSearchSortCtDef &sort_ctdef,
                       ObDASSearchCtx &ctx, ObDASSearchDriverIter *driver)
    : ObDASIterParam(DAS_ITER_SORT_TOPN), sort_ctdef_(sort_ctdef), ctx_(ctx), driver_(driver) {}
  /** @brief Check common parameters, the search driver, and whether DAS TopN is enabled. */
  bool is_valid() const override
  { return ObDASIterParam::is_valid() && driver_ != nullptr && sort_ctdef_.das_sort_topn_enable_; }
  const ObDASHybridSearchSortCtDef &sort_ctdef_; // Sort definition under Fusion.
  ObDASSearchCtx &ctx_; // Shared search context; must outlive this iterator.
  ObDASSearchDriverIter *driver_; // Search driver installed as the iterator's only child.
};

/**
 * @brief Fetch sort fields, apply search_after, and keep TopN rows from the search driver.
 * The first read consumes all candidates; results are then returned in sort order.
 * Parent merge iterators apply the final offset and limit.
 */
class ObDASSortTopNIter : public ObDASIter
{
public:
  /** @brief Create an uninitialized TopN iterator. */
  ObDASSortTopNIter();
  /** @brief Release the sorter before the private lookup expression context. */
  ~ObDASSortTopNIter();
  /** @brief Bind this execution's scan resources and open the driver unless size is zero. */
  int do_table_scan() override;
  /** @brief Clear previous results, refresh scan bindings, and restart with the fixed query configuration. */
  int rescan() override;
  /** @brief Return the borrowed search driver for inspecting the search tree. */
  ObDASSearchDriverIter *get_driver() const { return driver_; }
protected:
  /** @brief Prepare fixed parameters, lookup frames, expression layout and sorter once. */
  int inner_init(ObDASIterParam &param) override;
  /** @brief Clear results while keeping reusable lookup and driver resources. */
  int inner_reuse() override;
  /** @brief Close the lookup, destroy heap memory, and clear borrowed pointers. */
  int inner_release() override;
  /** @brief Reject single-row reads; this iterator requires batch reads. */
  int inner_get_next_row() override;
  /**
   * @brief Compute TopN on the first call and return up to capacity rows per call.
   * @param [out] count Returned row count; may be positive with OB_ITER_END.
   * Other errors are returned by later reads until restart or reuse.
   */
  int inner_get_next_rows(int64_t &count, int64_t capacity) override;
private:
  /** @brief Clear execution state, refresh lookup bindings and open or rescan the driver. */
  int start(bool rescan);
  /** @brief Clear this execution's results and status, retaining fixed sort configuration. */
  void reuse_sort_result();
  /**
   * @brief Read all candidates, fetch sort fields if needed, apply search_after, and fill the TopN heap.
   * When full, share the heap root's first sort value with the driver only for the duration of each read.
   */
  int consume();
  int prepare_sort_exprs();
  int collect_reset_exprs(ObExpr *expr);
  void clear_const_sort_expr_flags();
  void invalidate_sort_exprs();
  int restore_driver_batch(int64_t count);
  int process_batch(int64_t count);
  int project_output(int64_t count);
  ObDASSearchDriverIter *driver_; // Borrowed children_[0]; owned by the DAS tree.
  const ObDASHybridSearchSortCtDef *sort_ctdef_;
  ObDASSearchCtx *ctx_; // Borrowed runtime parameters and output expressions.
  ObDASSortValueLookupIter lookup_;
  ObDASHybridTopNImpl topn_impl_;
  ObEvalCtx *sort_eval_ctx_;
  // Iterator lifetime: selectors and array buffers survive batch/rescan reuse.
  common::ObArenaAllocator allocator_;
  // Plan expression pointers and private lookup frames remain stable until release.
  common::ObSEArray<ObExpr *, 8, common::ObIAllocator &> sort_exprs_;
  common::ObSEArray<ObExpr *, 16, common::ObIAllocator &> reset_exprs_;
  common::ObSEArray<ObExpr *, 16, common::ObIAllocator &> visited_exprs_;
  // One output batch. Datum payloads borrow the sorter's StoredRows.
  // Declaration/release order keeps topn_impl_ alive until these rows are destroyed.
  common::ObSEArray<ObDASSortTopNEntry, 16, common::ObIAllocator &> output_rows_;
  ObBitVector *skip_;
  uint16_t *selector_;
  int64_t emitted_rows_;
  bool started_; // Whether the driver has been opened and can be rescanned.
  bool lookup_inited_;
  bool consumed_; // Whether candidate reading is complete; also true when size is zero.
  int saved_error_; // Error returned by later reads until start() or reuse().
  common::ObOpProfile<common::ObMetric> *profile_; // Borrowed profile for this worker or partition execution.
};
} // namespace sql
} // namespace oceanbase
#endif
