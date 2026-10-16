/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */


/*
 * Overview
 * - Nested-query scope-boundary operator.
 *
 * Key Responsibilities
 * - Crosses the BASE↔NESTED scope boundary. The general inner subtree runs in
 *   DAS_SCOPE_NESTED; a non-scoring direct scalar or pure should/MSM=1 subtree
 *   under a BASE/uint64 boundary can instead run in BASE DOC_ID_ONLY mode. Scope is
 *   selected when the inner ops are created by
 *   ObDASNestedQueryRtDef::generate_op(), not at runtime.
 * - do_next_rowid: pull (doc_id, nested_key) from inner_op, truncate nested_key by BYTES_PER_LEVEL bytes
 *   (drop innermost level), deduplicate by the truncated rowid, emit to parent scope. At the outermost
 *   BASE boundary, the next call seeks directly to the following doc instead of draining duplicate
 *   nested-element matches. A multi-level NESTED boundary uses the same group-skip path for
 *   (doc_id, parent_prefix). DOC_ID_ONLY receives uint64 doc_id directly and only deduplicates it.
 * - do_advance_to: extend the target rowid with BYTES_PER_LEVEL bytes of 0x00 to form the inner target,
 *   then delegate to inner_op. DOC_ID_ONLY delegates the BASE target unchanged.
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_NESTED_QUERY_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_NESTED_QUERY_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_search_define.h"
#include "lib/allocator/page_arena.h"
#include "share/search_index/ob_search_index_encoder.h"

namespace oceanbase
{
namespace sql
{

// CtDef for the nested-query boundary node.
// Holds the nesting depth; the single inner subtree CtDef is stored in children_[0].
struct ObDASNestedQueryCtDef : ObIDASSearchCtDef
{
  OB_UNIS_VERSION(1);
public:
  explicit ObDASNestedQueryCtDef(common::ObIAllocator &alloc)
    : ObIDASSearchCtDef(alloc, DAS_OP_NESTED_QUERY),
      nesting_depth_(1),
      path_(),
      column_idx_(-1)
  {}
  ~ObDASNestedQueryCtDef() override = default;

  INHERIT_TO_STRING_KV("ObIDASSearchCtDef", ObIDASSearchCtDef,
                       K_(nesting_depth), K_(path), K_(column_idx));

  int32_t nesting_depth_;
  common::ObString path_; // added for child-domain must_not synthesis
  // JSON column ordinal in base table = search index rowkey col_idx. Used by
  // child-domain must_not synthesis to build the full-subtree scan range. -1 if unset.
  int64_t column_idx_;
};

// RtDef for the nested-query boundary node.
// generate_op() wraps the inner subtree's op in an ObDASNestedQueryOp.
struct ObDASNestedQueryRtDef : ObIDASSearchRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASNestedQueryRtDef()
    : ObIDASSearchRtDef(DAS_OP_NESTED_QUERY)
  {}
  ~ObDASNestedQueryRtDef() override = default;

  virtual int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override;
  virtual int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx,
                          ObIDASSearchOp *&op) override;

private:
  bool can_generate_doc_id_only(const ObDASSearchCtx &search_ctx) const;
  bool can_generate_parent_prefix_skip(const ObDASSearchCtx &search_ctx) const;
  OB_INLINE const ObDASNestedQueryCtDef *get_ctdef() const
  { return static_cast<const ObDASNestedQueryCtDef *>(ctdef_); }
  OB_INLINE ObIDASSearchRtDef *get_inner_rtdef() const
  { return (children_cnt_ > 0 && children_ != nullptr)
        ? static_cast<ObIDASSearchRtDef *>(children_[0]) : nullptr; }
};

class ObDASNestedQueryOpParam : public ObIDASSearchOpParam
{
public:
  ObDASNestedQueryOpParam(ObIDASSearchOp *inner_op,
                          int32_t nesting_depth,
                          bool is_doc_id_only,
                          bool enable_parent_prefix_skip)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_NESTED),
      inner_op_(inner_op),
      nesting_depth_(nesting_depth),
      is_doc_id_only_(is_doc_id_only),
      enable_parent_prefix_skip_(enable_parent_prefix_skip) {}
  ~ObDASNestedQueryOpParam() override = default;

  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  { return children.push_back(inner_op_); }

  INHERIT_TO_STRING_KV("ObIDASSearchOpParam", ObIDASSearchOpParam,
                       KP(inner_op_), K(nesting_depth_), K(is_doc_id_only_),
                       K(enable_parent_prefix_skip_));

  ObIDASSearchOp *inner_op_;
  int32_t nesting_depth_; // 1 = single-level nested (truncate BYTES_PER_LEVEL bytes), 2 = double-level, etc.
  bool is_doc_id_only_;
  bool enable_parent_prefix_skip_;
};

/*
 * ObDASNestedQueryOp — scope-boundary operator.
 *
 * Responsibilities:
 * - do_init: bind inner_op_, nesting_depth_, and the creation-time DOC_ID_ONLY decision.
 * - do_next_rowid: pull (doc_id, nested_key) from inner_op, truncate nested_key by BYTES_PER_LEVEL bytes
 *   (drop innermost level), deduplicate by the truncated rowid, emit to parent scope.
 *   When parent scope is BASE, emit plain uint64 doc_id. When parent scope is NESTED
 *   (multi-layer nesting), emit COMPACT with the truncated nk. A BASE/uint64 boundary over a
 *   NESTED/COMPACT child seeks to (last_doc_id + 1, minimum nested key) on the next call.
 *   Multi-level boundaries reuse the same group state to seek past a parent prefix.
 *   DOC_ID_ONLY consumes a BASE/uint64 child directly and seeks with a BASE target.
 * - do_advance_to: extend the target rowid with BYTES_PER_LEVEL bytes of 0x00 to form the inner target,
 *   then delegate to inner_op.
 * - do_advance_shallow: falls back to base default (degrades to advance_to). Nested
 *   queries are filter-only with no score upper bound.
 */
class ObDASNestedQueryOp final : public ObIDASSearchOp
{
private:
  // A group prefix is the nested key after dropping its innermost level.
  // A full nested key has at most MAX_DEPTH fixed-width components, so the
  // retained prefix needs at most (MAX_DEPTH - 1) * BYTES_PER_LEVEL bytes.
  static const int64_t MAX_GROUP_PREFIX_LEN = (ObSearchIndexNestedKeyEncoder::MAX_DEPTH - 1)
    * ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL;
public:
  explicit ObDASNestedQueryOp(ObDASSearchCtx &ctx);
  ~ObDASNestedQueryOp() override;

  VIRTUAL_TO_STRING_KV(K_(op_id), KP_(inner_op), K_(nesting_depth),
                       K_(is_doc_id_only), K_(enable_group_skip),
                       K_(has_pending_group_skip));

protected:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  // do_advance_shallow inherits base default (falls back to advance_to).

private:
  // Pack [doc_id, nested_key] into a NESTED/COMPACT RowID.
  int build_nested_rowid(const ObDatum &doc_id_datum,
                         const ObDatum &nested_key_datum,
                         ObDASRowID &rowid,
                         common::ObIAllocator &allocator);
  // Truncate the inner rowid by one nesting level and build the output rowid
  // in this op's output scope. This does not touch streaming dedup state.
  int build_truncated_rowid(const ObDASRowID &inner_rowid,
                            ObDASRowID &emitted,
                            common::ObIAllocator &allocator);
  // Truncate the inner rowid by one nesting level (BYTES_PER_LEVEL from the end of nested_key),
  // produce the rowid in this op's output scope (BASE or NESTED), and check for
  // duplicate against last_emitted_rowid_.
  // Sets is_dup=true if the truncated rowid equals last_emitted_rowid_.
  int truncate_and_emit(const ObDASRowID &inner_rowid, ObDASRowID &emitted, bool &is_dup);
  // BASE scope with uint64 RowID only needs doc_id for streaming dedup. DOC_ID_ONLY
  // passes through its BASE child RowID; the normal nested path validates and
  // drops nested_key without materializing a Compact RowID.
  int emit_base_rowid_fast(const ObDASRowID &inner_rowid,
                           ObDASRowID &emitted,
                           bool &is_dup);
  // Build the minimum NESTED-scope target for a BASE doc_id: (doc_id, 0x00000000).
  int build_inner_doc_target(uint64_t doc_id,
                             ObDASRowID &inner_target,
                             common::ObIAllocator &allocator);
  // Build the first child key strictly after the pending output group. A BASE
  // group advances to the next doc; a NESTED group advances its parent prefix.
  int build_next_group_inner_target(ObDASRowID &inner_target,
                                    common::ObIAllocator &allocator);
  // Probe one child row after an emitted group. Return it directly when it
  // belongs to a later group; otherwise seek to the strict group successor.
  int advance_to_next_group(ObDASRowID &next_id, double &score);
  // Compare the output group represented by an inner RowID with the pending group.
  int compare_inner_group_with_pending(const ObDASRowID &inner, int &cmp);
  // Reuse the normal truncate/dedup state machine to materialize a selected candidate.
  int emit_group_candidate(const ObDASRowID &inner, ObDASRowID &emitted);
  // External advance_to() and internal group skipping have separate semantics. Remember only
  // the group returned by this boundary; do not reuse streaming-dedup state as a pending target.
  int remember_for_next_skip(const ObDASRowID &emitted);
  // Increment a fixed-width big-endian parent prefix. Returns false on all-0xFF overflow.
  static bool increment_parent_prefix(char *prefix, int64_t prefix_len);
  OB_INLINE void clear_pending_group_skip()
  {
    pending_group_doc_id_ = 0;
    has_pending_group_skip_ = false;
  }
  void reset_iteration_state();

private:
  OB_INLINE common::ObArenaAllocator &current_dedup_allocator()
  { return current_allocator_is_a_ ? dedup_allocator_a_ : dedup_allocator_b_; }
  OB_INLINE common::ObArenaAllocator &next_dedup_allocator()
  { return current_allocator_is_a_ ? dedup_allocator_b_ : dedup_allocator_a_; }

private:
  ObIDASSearchOp *inner_op_;
  int32_t nesting_depth_;
  bool is_doc_id_only_;
  // Cached at init from the immutable operator/RowID shape.
  bool enable_group_skip_;
  uint64_t pending_group_doc_id_;
  char pending_group_prefix_[MAX_GROUP_PREFIX_LEN];
  ObDASRowID last_emitted_rowid_;
  bool has_emitted_;
  bool has_pending_group_skip_;
  // Ping-pong allocators: only the single live last_emitted_rowid_ buffer is retained at
  // any time. next_dedup_allocator() receives the new emit; on commit, current is reset and
  // roles swap. Both are reset on close/rescan.
  common::ObArenaAllocator dedup_allocator_a_;
  common::ObArenaAllocator dedup_allocator_b_;
  bool current_allocator_is_a_; // true → live emit is in a, scratch is b
  // Scratch allocator for inner_target construction in do_advance_to; reset at call start.
  common::ObArenaAllocator advance_scratch_allocator_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_NESTED_QUERY_OP_H_
