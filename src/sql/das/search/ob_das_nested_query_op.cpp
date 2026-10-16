/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/search/ob_das_nested_query_op.h"
#include "sql/das/search/ob_das_boolean_query.h"
#include "sql/das/search/ob_das_search_context.h"
#include "lib/utility/ob_print_utils.h"
#include "share/search_index/ob_search_index_encoder.h"

namespace oceanbase
{
namespace sql
{

namespace {
struct NestedRowIDDump
{
  NestedRowIDDump(ObDASSearchCtx &ctx, ObDASScopeId scope, const ObDASRowID &rid)
    : ctx_(ctx), scope_(scope), rid_(rid) {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (rid_.is_min()) { common::databuff_printf(buf, buf_len, pos, "MIN"); return pos; }
    if (rid_.is_max()) { common::databuff_printf(buf, buf_len, pos, "MAX"); return pos; }
    ObDatum d0;
    int tret = ctx_.get_datum_from_rowid(scope_, rid_, d0, 0);
    if (OB_SUCCESS != tret) { common::databuff_printf(buf, buf_len, pos, "<col0_err=%d>", tret); return pos; }
    common::databuff_printf(buf, buf_len, pos, "doc_id=%lu", d0.get_uint());
    if (scope_ == DAS_SCOPE_NESTED) {
      ObDatum d1;
      tret = ctx_.get_datum_from_rowid(scope_, rid_, d1, 1);
      if (OB_SUCCESS != tret) {
        common::databuff_printf(buf, buf_len, pos, ",nk=<col1_err=%d>", tret);
      } else {
        common::databuff_printf(buf, buf_len, pos, ",nk_len=%u,nk_hex=", d1.len_);
        const unsigned char *p = reinterpret_cast<const unsigned char *>(d1.ptr_);
        for (uint32_t i = 0; i < d1.len_ && pos + 2 < buf_len; ++i) {
          common::databuff_printf(buf, buf_len, pos, "%02X", p[i]);
        }
      }
    }
    return pos;
  }
  ObDASSearchCtx &ctx_;
  ObDASScopeId scope_;
  const ObDASRowID &rid_;
};

// Flattening or group skipping may discard matching rows, so it is safe only
// when the node neither contributes a score nor drives top-level score pruning.
bool is_non_scoring_search_ctdef(const ObDASBaseCtDef *ctdef)
{
  bool bret = OB_NOT_NULL(ctdef) && ctdef->is_search_ctdef();
  if (bret) {
    const ObIDASSearchCtDef *search_ctdef =
        static_cast<const ObIDASSearchCtDef *>(ctdef);
    bret = !search_ctdef->is_scoring() && !search_ctdef->is_top_level_scoring();
  }
  return bret;
}

bool can_generate_doc_id_only_subtree(const ObDASBaseRtDef *rtdef);

// Projecting children to doc_id loses which nested element matched. That is
// safe for one required clause, or for a pure OR with minimum_should_match=1;
// multiple required clauses and exclusions still need nested-element identity.
bool can_generate_doc_id_only_boolean(const ObDASBooleanQueryCtDef &ctdef,
                                      const ObDASBaseRtDef &rtdef)
{
  // String MSM is evaluated against the runtime should-clause count. Its
  // static snapshot cannot prove that flattening preserves nested-element
  // correlation, so keep the subtree in NESTED scope.
  bool bret = !ctdef.is_msm_unresolved_expr()
      && OB_NOT_NULL(ctdef.children_)
      && OB_NOT_NULL(rtdef.children_);
  ObBooleanSubClause<ObIDASSearchCtDef> must;
  ObBooleanSubClause<ObIDASSearchCtDef> filter;
  ObBooleanSubClause<ObIDASSearchCtDef> should;
  ObBooleanSubClause<ObIDASSearchCtDef> must_not;
  if (bret) {
    bret = OB_SUCCESS == ctdef.must(must)
        && OB_SUCCESS == ctdef.filter(filter)
        && OB_SUCCESS == ctdef.should(should)
        && OB_SUCCESS == ctdef.must_not(must_not);
  }
  if (bret) {
    const int64_t required_count = must.count() + filter.count();
    const bool is_single_required =
        1 == required_count
        && should.empty()
        && 0 == ctdef.get_min_should_match();
    const bool is_pure_should =
        0 == required_count
        && !should.empty()
        && 1 == ctdef.get_min_should_match();
    const int64_t positive_count = required_count + should.count();
    bret = must_not.empty()
        && (is_single_required || is_pure_should)
        && positive_count == ctdef.children_cnt_
        && positive_count == rtdef.children_cnt_;
  }
  for (int64_t i = 0; bret && i < rtdef.children_cnt_; ++i) {
    bret = can_generate_doc_id_only_subtree(rtdef.children_[i]);
  }
  return bret;
}

// Walk the executable subtree using rtdef->ctdef_ as its canonical definition.
// Scalar is the supported leaf; Boolean is accepted only when all descendants
// can drop nested-element identity without changing Boolean semantics.
bool can_generate_doc_id_only_subtree(const ObDASBaseRtDef *rtdef)
{
  const ObDASBaseCtDef *ctdef = OB_NOT_NULL(rtdef) ? rtdef->ctdef_ : nullptr;
  bool bret = false;
  if (is_non_scoring_search_ctdef(ctdef)) {
    if (DAS_OP_SCALAR_QUERY == ctdef->op_type_) {
      // A scalar query delegates structural validation to its generate_op().
      bret = true;
    } else if (DAS_OP_BOOLEAN_QUERY == ctdef->op_type_) {
      bret = can_generate_doc_id_only_boolean(
          *static_cast<const ObDASBooleanQueryCtDef *>(ctdef), *rtdef);
    }
  }
  return bret;
}
}  // namespace

OB_SERIALIZE_MEMBER((ObDASNestedQueryCtDef, ObIDASSearchCtDef), nesting_depth_, path_, column_idx_);

OB_SERIALIZE_MEMBER((ObDASNestedQueryRtDef, ObIDASSearchRtDef));

int ObDASNestedQueryRtDef::compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  // Propagate cost from the single inner child.
  ObIDASSearchRtDef *inner = get_inner_rtdef();
  if (OB_ISNULL(inner)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nested query rtdef has no inner child", KR(ret));
  } else if (OB_FAIL(inner->get_cost(search_ctx, cost))) {
    LOG_WARN("failed to get inner cost for nested query", KR(ret));
  }
  return ret;
}

// At a BASE boundary, avoid entering NESTED scope when the entire non-scoring
// subtree can be evaluated by doc_id alone.
bool ObDASNestedQueryRtDef::can_generate_doc_id_only(const ObDASSearchCtx &search_ctx) const
{
  const ObDASNestedQueryCtDef *ctdef = get_ctdef();
  return search_ctx.get_op_creating_scope() == DAS_SCOPE_BASE
      && search_ctx.get_rowid_type() == DAS_ROWID_TYPE_UINT64
      && is_non_scoring_search_ctdef(ctdef)
      && can_generate_doc_id_only_subtree(get_inner_rtdef());
}

// A nested() below another nested() emits one row per parent prefix. Once a
// non-scoring parent is emitted, the remaining descendants with that prefix
// can be skipped. The generated operator validates the actual RowID shape.
bool ObDASNestedQueryRtDef::can_generate_parent_prefix_skip(const ObDASSearchCtx &search_ctx) const
{
  const ObDASNestedQueryCtDef *ctdef = get_ctdef();
  ObIDASSearchRtDef *inner_rtdef = get_inner_rtdef();
  return search_ctx.get_op_creating_scope() == DAS_SCOPE_NESTED
      && ctdef->nesting_depth_ > 1
      && ctdef->nesting_depth_ <= ObSearchIndexNestedKeyEncoder::MAX_DEPTH
      && !ctdef->is_scoring()
      && !ctdef->is_top_level_scoring()
      && is_non_scoring_search_ctdef(inner_rtdef->ctdef_);
}

// ScopePropagateGuard is responsible for propagating scope to children
struct ScopePropagateGuard
{
  ScopePropagateGuard(ObDASSearchCtx &search_ctx)
    : search_ctx_(search_ctx) {
      search_ctx_.enter_scope_for_op_creating(ObDASScopeId::DAS_SCOPE_NESTED, old_scope_);
  }
  ~ScopePropagateGuard() {
    search_ctx_.exit_scope_after_op_created(old_scope_);
  }
  ObDASSearchCtx &search_ctx_;
  ObDASScopeId old_scope_;
};

int ObDASNestedQueryRtDef::generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx,
                                       ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASNestedQueryCtDef *ctdef = get_ctdef();
  ObIDASSearchRtDef *inner_rtdef = get_inner_rtdef();
  ObIDASSearchOp *inner_op = nullptr;
  bool is_doc_id_only = false;
  bool enable_parent_prefix_skip = false;
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nested query ctdef is null", KR(ret));
  } else if (OB_ISNULL(inner_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nested query inner rtdef is null", KR(ret));
  } else {
    is_doc_id_only = can_generate_doc_id_only(search_ctx);
    enable_parent_prefix_skip =
        !is_doc_id_only && can_generate_parent_prefix_skip(search_ctx);
    if (is_doc_id_only) {
      if (OB_FAIL(inner_rtdef->generate_op(lead_cost, search_ctx, inner_op))) {
        LOG_WARN("failed to generate DOC_ID_ONLY subtree for nested query", KR(ret));
      }
    } else {
      ScopePropagateGuard guard(search_ctx);
      if (OB_FAIL(inner_rtdef->generate_op(lead_cost, search_ctx, inner_op))) {
        LOG_WARN("failed to generate inner op for nested query", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(inner_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner op is null", KR(ret), K(is_doc_id_only));
    } else {
      LOG_TRACE("generated nested query inner op",
          K(is_doc_id_only), K(enable_parent_prefix_skip),
          K(inner_op->get_scope()), K(inner_op->get_rowid_type()), K(inner_op->get_op_type()));
    }
  }
  if (OB_SUCC(ret)) {
    ObDASNestedQueryOpParam op_param(
        inner_op, ctdef->nesting_depth_, is_doc_id_only, enable_parent_prefix_skip);
    ObDASNestedQueryOp *nested_op = nullptr;
    if (OB_FAIL(search_ctx.create_op(op_param, nested_op))) {
      LOG_WARN("failed to create nested query op", KR(ret));
    } else if (OB_ISNULL(nested_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nested query op is null", KR(ret));
    } else {
      op = static_cast<ObIDASSearchOp *>(nested_op);
    }
  }
  return ret;
}

ObDASNestedQueryOp::ObDASNestedQueryOp(ObDASSearchCtx &ctx)
  : ObIDASSearchOp(ctx),
    inner_op_(nullptr),
    nesting_depth_(0),
    is_doc_id_only_(false),
    enable_group_skip_(false),
    pending_group_doc_id_(0),
    last_emitted_rowid_(),
    has_emitted_(false),
    has_pending_group_skip_(false),
    dedup_allocator_a_("DASDedupA"),
    dedup_allocator_b_("DASDedupB"),
    current_allocator_is_a_(true),
    advance_scratch_allocator_("DASAdvScratch")
{
}

ObDASNestedQueryOp::~ObDASNestedQueryOp() = default;

int ObDASNestedQueryOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(DAS_SEARCH_OP_NESTED != op_param.get_op_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected op param type", K(ret), K(op_param.get_op_type()));
  } else {
    const ObDASNestedQueryOpParam &p = static_cast<const ObDASNestedQueryOpParam&>(op_param);
    inner_op_ = p.inner_op_;
    nesting_depth_ = p.nesting_depth_;
    is_doc_id_only_ = p.is_doc_id_only_;
    enable_group_skip_ = false;
    reset_iteration_state();
    // DOC_ID_ONLY keeps both the boundary and its child in BASE/UINT64. The
    // regular path propagates NESTED/COMPACT only to the child.
    const ObDASScopeId expected_inner_scope =
        is_doc_id_only_ ? DAS_SCOPE_BASE : DAS_SCOPE_NESTED;
    const ObDASRowIDType expected_inner_rowid_type =
        is_doc_id_only_ ? DAS_ROWID_TYPE_UINT64 : DAS_ROWID_TYPE_COMPACT;
    const bool output_shape_valid =
        !is_doc_id_only_
        || (scope_ == DAS_SCOPE_BASE && get_rowid_type() == DAS_ROWID_TYPE_UINT64);
    if (OB_ISNULL(inner_op_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("nested query op requires non-null inner_op", K(ret));
    } else if (OB_UNLIKELY(nesting_depth_ <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("nested query op requires positive nesting depth", K(ret), K(nesting_depth_));
    } else if (OB_UNLIKELY(
                   !output_shape_valid
                   || inner_op_->get_scope() != expected_inner_scope
                   || inner_op_->get_rowid_type() != expected_inner_rowid_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nested query scope or rowid type",
          K(ret), K_(is_doc_id_only), K(scope_), K(get_rowid_type()),
          K(inner_op_->get_scope()), K(inner_op_->get_rowid_type()),
          K(expected_inner_scope), K(expected_inner_rowid_type));
    } else {
      // Cache the two supported group shapes:
      // 1. Outermost nested(path="$.comments"):
      //      inner (doc=7, nk=[3]), (doc=7, nk=[4]) -> output doc=7
      //    If the lookahead is still in doc 7, seek to (doc=8, minimum nested key).
      // 2. nested(path="$.comments.replies") below nested(path="$.comments"):
      //      inner (doc=7, nk=[3,1]), (doc=7, nk=[3,2]) -> output (doc=7, nk=[3])
      //    If the lookahead still has prefix [3], seek to (doc=7, nk=[4,0]).
      // p.enable_parent_prefix_skip_ proves that case 2 is non-scoring and depth-safe;
      // do_init only binds that decision to the output shape of the created operator.
      const bool can_skip_doc_group = scope_ == DAS_SCOPE_BASE
          && get_rowid_type() == DAS_ROWID_TYPE_UINT64;
      const bool can_skip_parent_group = p.enable_parent_prefix_skip_
          && scope_ == DAS_SCOPE_NESTED
          && get_rowid_type() == DAS_ROWID_TYPE_COMPACT;
      enable_group_skip_ = can_skip_doc_group || can_skip_parent_group;
    }
  }
  return ret;
}

int ObDASNestedQueryOp::do_open()
{
  int ret = OB_SUCCESS;
  reset_iteration_state();
  if (OB_ISNULL(inner_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_op_ is null", K(ret));
  } else if (OB_FAIL(inner_op_->open())) {
    LOG_WARN("failed to open inner_op", K(ret));
  }
  return ret;
}

int ObDASNestedQueryOp::do_close()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(inner_op_)) {
    if (OB_FAIL(inner_op_->close())) {
      LOG_WARN("failed to close inner_op", K(ret));
    }
  }
  // Always reset our state regardless of close result.
  reset_iteration_state();
  return ret;
}

int ObDASNestedQueryOp::do_rescan()
{
  int ret = OB_SUCCESS;
  reset_iteration_state();
  if (OB_NOT_NULL(inner_op_)) {
    if (OB_FAIL(inner_op_->rescan())) {
      LOG_WARN("failed to rescan inner_op", K(ret));
    }
  }
  return ret;
}

int ObDASNestedQueryOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inner_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_op_ is null", K(ret));
  } else if (has_pending_group_skip_) {
    if (OB_FAIL(advance_to_next_group(next_id, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance inner op to next group", K(ret));
    }
  } else {
    bool emitted = false;
    while (OB_SUCC(ret) && !emitted) {
      ObDASRowID inner_id;
      double inner_score = 0.0;
      if (OB_FAIL(inner_op_->next_rowid(inner_id, inner_score))) {
        // OB_ITER_END propagates up unchanged
        LOG_TRACE("[NESTED-DBG] inner_op next_rowid RET",
                 KR(ret), K(inner_op_->get_scope()),
                 K(inner_op_->get_op_type()));
      } else {
        LOG_TRACE("[NESTED-DBG] inner_op next_rowid OK",
                 K(inner_op_->get_scope()),
                 K(inner_op_->get_rowid_type()), K(inner_op_->get_op_type()),
                 "inner_id", NestedRowIDDump(search_ctx_, inner_op_->get_scope(), inner_id));
        ObDASRowID truncated;
        bool is_dup = false;
        if (OB_FAIL(truncate_and_emit(inner_id, truncated, is_dup))) {
          LOG_WARN("failed to truncate and emit", K(ret));
        } else if (is_dup) {
          continue; // skip duplicate, fetch next
        } else {
          next_id = truncated;
          score = inner_score;
          emitted = true;
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(remember_for_next_skip(next_id))) {
    LOG_WARN("failed to remember emitted group for next skip", K(ret));
  }
  return ret;
}

int ObDASNestedQueryOp::build_nested_rowid(
    const ObDatum &doc_id_datum,
    const ObDatum &nested_key_datum,
    ObDASRowID &rowid,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDatum, 2> datums;
  if (OB_FAIL(datums.push_back(doc_id_datum))) {
    LOG_WARN("failed to push doc id datum", K(ret));
  } else if (OB_FAIL(datums.push_back(nested_key_datum))) {
    LOG_WARN("failed to push nested key datum", K(ret));
  } else if (OB_FAIL(search_ctx_.write_datums_to_rowid(
                 DAS_SCOPE_NESTED, datums, rowid, allocator))) {
    LOG_WARN("failed to build nested rowid", K(ret));
  }
  return ret;
}

// The NESTED scope compact rowid holds: col 0 = doc_id (uint64), col 1 = nested_key (binary string).
// We truncate the last BYTES_PER_LEVEL bytes of nested_key, then emit in this op's output scope (scope_).
// If scope_ == DAS_SCOPE_BASE: emit plain uint64 doc_id.
// If scope_ == DAS_SCOPE_NESTED: emit compact (doc_id, truncated_nk).
int ObDASNestedQueryOp::build_truncated_rowid(const ObDASRowID &inner_rowid,
                                              ObDASRowID &emitted,
                                              common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  // Extract doc_id datum (col 0) and nested_key datum (col 1) from the NESTED-scope rowid.
  ObDatum doc_id_datum;
  ObDatum nk_datum;
  if (OB_FAIL(search_ctx_.get_datum_from_rowid(DAS_SCOPE_NESTED, inner_rowid, doc_id_datum, 0))) {
    LOG_WARN("failed to get doc_id datum from nested rowid", K(ret));
  } else if (OB_FAIL(search_ctx_.get_datum_from_rowid(DAS_SCOPE_NESTED, inner_rowid, nk_datum, 1))) {
    LOG_WARN("failed to get nested_key datum from nested rowid", K(ret));
  } else if (OB_UNLIKELY(nk_datum.len_ < ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("malformed nested_key: length < less than BYTES_PER_LEVEL", K(ret),
        K(nk_datum.len_), K(ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL));
  } else {
    LOG_TRACE("[NESTED-DBG] truncate_and_emit IN", K(ret),
             K(scope_), K(doc_id_datum.get_uint()),
             K(nk_datum.len_),
             "inner_rowid", NestedRowIDDump(search_ctx_, DAS_SCOPE_NESTED, inner_rowid));
    // Truncated nested_key: drop last BYTES_PER_LEVEL bytes.
    ObDatum truncated_nk;
    truncated_nk.ptr_ = nk_datum.ptr_;
    truncated_nk.pack_ = nk_datum.pack_;
    truncated_nk.len_ = nk_datum.len_ - ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL; // len_ is the lower 29 bits of pack_

    if (scope_ == DAS_SCOPE_BASE) {
      // Parent expects plain uint64 doc_id.
      emitted.set_uint64(doc_id_datum.get_uint());
    } else {
      // Parent expects COMPACT (doc_id, truncated_nk).
      if (OB_FAIL(build_nested_rowid(
              doc_id_datum, truncated_nk, emitted, allocator))) {
        LOG_WARN("failed to build truncated nested rowid", K(ret));
      }
    }
  }
  return ret;
}

int ObDASNestedQueryOp::truncate_and_emit(const ObDASRowID &inner_rowid, ObDASRowID &emitted, bool &is_dup)
{
  int ret = OB_SUCCESS;
  is_dup = false;

  if (scope_ == DAS_SCOPE_BASE && get_rowid_type() == DAS_ROWID_TYPE_UINT64) {
    if (OB_FAIL(emit_base_rowid_fast(inner_rowid, emitted, is_dup))) {
      LOG_WARN("failed to emit base rowid with fast dedup", K(ret));
    }
  } else {
    // Build the output rowid into next_dedup_allocator() (ping-pong: the other slot).
    common::ObArenaAllocator &next_alloc = next_dedup_allocator();
    next_alloc.reset();
    if (OB_FAIL(build_truncated_rowid(inner_rowid, emitted, next_alloc))) {
      LOG_WARN("failed to build truncated rowid", K(ret));
    }

    if (OB_SUCC(ret)) {
      // Dedup check against last_emitted_rowid_.
      if (has_emitted_) {
        int cmp = 0;
        if (OB_FAIL(compare_rowid(last_emitted_rowid_, emitted, cmp))) {
          LOG_WARN("failed to compare rowids for dedup", K(ret));
        } else if (cmp == 0) {
          is_dup = true;
        }
      }

      if (OB_SUCC(ret) && !is_dup) {
        // Commit: reset the now-stale current allocator, flip roles, record new emit.
        // After this point last_emitted_rowid_ points into what was next_dedup_allocator(),
        // which becomes current_dedup_allocator() after the flip.
        current_dedup_allocator().reset();
        current_allocator_is_a_ = !current_allocator_is_a_;
        last_emitted_rowid_ = emitted;
        has_emitted_ = true;
      }
    }
  }
  return ret;
}

int ObDASNestedQueryOp::emit_base_rowid_fast(
    const ObDASRowID &inner_rowid,
    ObDASRowID &emitted,
    bool &is_dup)
{
  int ret = OB_SUCCESS;
  ObDatum doc_id_datum;
  is_dup = false;
  if (is_doc_id_only_) {
    // UInt64 RowID is stored inline, so a DOC_ID_ONLY child can pass through.
    emitted = inner_rowid;
  } else if (OB_FAIL(search_ctx_.get_datum_from_rowid(
                 DAS_SCOPE_NESTED, inner_rowid, doc_id_datum, 0))) {
    LOG_WARN("failed to get doc id from nested rowid", K(ret));
  } else {
    emitted.set_uint64(doc_id_datum.get_uint());
  }
  if (OB_SUCC(ret) && has_emitted_) {
    int cmp = 0;
    if (OB_FAIL(compare_rowid(last_emitted_rowid_, emitted, cmp))) {
      LOG_WARN("failed to compare base rowids for dedup", K(ret));
    } else {
      is_dup = 0 == cmp;
    }
  }
  if (OB_SUCC(ret) && !is_dup) {
    last_emitted_rowid_ = emitted;
    has_emitted_ = true;
  }
  return ret;
}

int ObDASNestedQueryOp::build_inner_doc_target(
    const uint64_t doc_id,
    ObDASRowID &inner_target,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  static const char kMinNestedKey[ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL] = {0, 0, 0, 0};
  ObDatum doc_id_datum;
  ObDatum min_nk_datum;
  doc_id_datum.ptr_ = reinterpret_cast<const char *>(&doc_id);
  doc_id_datum.pack_ = sizeof(doc_id);
  min_nk_datum.ptr_ = kMinNestedKey;
  min_nk_datum.pack_ = ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL;
  if (OB_FAIL(build_nested_rowid(
          doc_id_datum, min_nk_datum, inner_target, allocator))) {
    LOG_WARN("failed to build minimum inner target for doc", K(ret), K(doc_id));
  }
  return ret;
}

// Advance a big-endian encoded prefix to the first key outside its current
// group. An all-0xFF prefix has no finite successor.
bool ObDASNestedQueryOp::increment_parent_prefix(char *prefix, const int64_t prefix_len)
{
  bool advanced = false;
  if (OB_NOT_NULL(prefix) && prefix_len > 0) {
    for (int64_t i = prefix_len - 1; !advanced && i >= 0; --i) {
      unsigned char &byte = reinterpret_cast<unsigned char *>(prefix)[i];
      if (0xFF == byte) {
        byte = 0;
      } else {
        ++byte;
        advanced = true;
      }
    }
  }
  return advanced;
}

int ObDASNestedQueryOp::build_next_group_inner_target(
    ObDASRowID &inner_target,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t bytes_per_level = ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL;
  const int64_t expected_parent_len =
      static_cast<int64_t>(nesting_depth_ - 1) * bytes_per_level;
  const int64_t inner_nk_len =
      static_cast<int64_t>(nesting_depth_) * bytes_per_level;
  if (scope_ == DAS_SCOPE_BASE) {
    if (UINT64_MAX == pending_group_doc_id_) {
      ret = OB_ITER_END;
    } else {
      const uint64_t next_doc_id = pending_group_doc_id_ + 1;
      if (is_doc_id_only_) {
        inner_target.set_uint64(next_doc_id);
      } else if (OB_FAIL(build_inner_doc_target(
                     next_doc_id, inner_target, allocator))) {
        LOG_WARN("failed to build nested next-doc target",
            K(ret), K(next_doc_id));
      }
    }
  } else {
    char *inner_nk_buf = static_cast<char *>(allocator.alloc(inner_nk_len));
    if (OB_ISNULL(inner_nk_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate next-parent inner key", K(ret), K(inner_nk_len));
    } else {
      MEMCPY(inner_nk_buf, pending_group_prefix_, expected_parent_len);
      const bool has_prefix_successor =
          increment_parent_prefix(inner_nk_buf, expected_parent_len);
      uint64_t target_doc_id = pending_group_doc_id_;
      if (has_prefix_successor) {
        MEMSET(inner_nk_buf + expected_parent_len, 0, bytes_per_level);
      } else if (UINT64_MAX == target_doc_id) {
        ret = OB_ITER_END;
      } else {
        ++target_doc_id;
        MEMSET(inner_nk_buf, 0, inner_nk_len);
      }

      if (OB_SUCC(ret)) {
        ObDatum target_doc_datum;
        ObDatum target_nk_datum;
        target_doc_datum.ptr_ = reinterpret_cast<const char *>(&target_doc_id);
        target_doc_datum.pack_ = sizeof(target_doc_id);
        target_nk_datum.ptr_ = inner_nk_buf;
        target_nk_datum.pack_ = static_cast<uint32_t>(inner_nk_len);
        if (OB_FAIL(build_nested_rowid(
                target_doc_datum, target_nk_datum, inner_target, allocator))) {
          LOG_WARN("failed to build next-parent inner target",
              K(ret), K(target_doc_id), K(inner_nk_len));
        }
      }
    }
  }
  return ret;
}

int ObDASNestedQueryOp::compare_inner_group_with_pending(
    const ObDASRowID &inner,
    int &cmp)
{
  int ret = OB_SUCCESS;
  ObDatum doc_id_datum;
  ObDatum nk_datum;
  const ObDASScopeId inner_scope =
      is_doc_id_only_ ? DAS_SCOPE_BASE : DAS_SCOPE_NESTED;
  const int64_t pending_prefix_len = scope_ == DAS_SCOPE_NESTED
      ? static_cast<int64_t>(nesting_depth_ - 1)
          * ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL
      : 0;
  if (OB_FAIL(search_ctx_.get_datum_from_rowid(
          inner_scope, inner, doc_id_datum, 0))) {
    LOG_WARN("failed to get doc id from inner rowid", K(ret));
  } else {
    const uint64_t inner_doc_id = doc_id_datum.get_uint();
    if (pending_group_doc_id_ < inner_doc_id) {
      cmp = -1;
    } else if (pending_group_doc_id_ > inner_doc_id) {
      cmp = 1;
    } else if (scope_ == DAS_SCOPE_BASE) {
      cmp = 0;
    } else if (OB_FAIL(search_ctx_.get_datum_from_rowid(
                   DAS_SCOPE_NESTED, inner, nk_datum, 1))) {
      LOG_WARN("failed to get nested key from inner rowid", K(ret));
    } else if (OB_UNLIKELY(
                   nk_datum.len_ < ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("malformed inner nested key while comparing group",
          K(ret), K(nk_datum.len_));
    } else {
      const int64_t inner_parent_len =
          nk_datum.len_ - ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL;
      const int64_t common_len =
          pending_prefix_len < inner_parent_len
              ? pending_prefix_len : inner_parent_len;
      cmp = 0 == common_len
          ? 0
          : MEMCMP(pending_group_prefix_, nk_datum.ptr_, common_len);
      if (0 == cmp) {
        cmp = pending_prefix_len < inner_parent_len
            ? -1
            : (pending_prefix_len > inner_parent_len ? 1 : 0);
      }
    }
  }
  return ret;
}

int ObDASNestedQueryOp::emit_group_candidate(
    const ObDASRowID &inner,
    ObDASRowID &emitted)
{
  int ret = OB_SUCCESS;
  bool is_dup = false;
  if (OB_FAIL(truncate_and_emit(inner, emitted, is_dup))) {
    LOG_WARN("failed to emit group candidate", K(ret));
  } else if (OB_UNLIKELY(is_dup)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("strict group seek returned duplicate candidate", K(ret));
  }
  return ret;
}

int ObDASNestedQueryOp::advance_to_next_group(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  const int64_t pending_prefix_len = scope_ == DAS_SCOPE_NESTED
      ? static_cast<int64_t>(nesting_depth_ - 1)
          * ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL
      : 0;
  // Keep the inline pending doc/prefix bytes valid for this call, but consume the
  // pending marker now so an error cannot accidentally repeat the same skip.
  has_pending_group_skip_ = false;
  advance_scratch_allocator_.reset();

  ObDASRowID probe_inner_id;
  double probe_score = 0.0;
  if (OB_FAIL(inner_op_->next_rowid(probe_inner_id, probe_score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to probe inner op after emitted group", K(ret));
  } else {
    int cmp = 0;
    if (OB_FAIL(compare_inner_group_with_pending(probe_inner_id, cmp))) {
      LOG_WARN("failed to compare pending group with probe", K(ret));
    } else if (OB_UNLIKELY(cmp > 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner op returned decreasing group rowid", K(ret),
          K(pending_group_doc_id_), K(pending_prefix_len),
          "probe_inner", NestedRowIDDump(
              search_ctx_, inner_op_->get_scope(), probe_inner_id));
    } else if (cmp < 0) {
      // The common fanout=1 case: the probe already belongs to a later group.
      if (OB_FAIL(emit_group_candidate(probe_inner_id, next_id))) {
        LOG_WARN("failed to commit probed group", K(ret));
      } else {
        score = probe_score;
      }
    } else {
      // A duplicate confirms multiple child rows under this output group. Seek
      // to the first key in the strict successor group.
      ObDASRowID inner_target;
      if (OB_FAIL(build_next_group_inner_target(
              inner_target, advance_scratch_allocator_))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to build next-group target", K(ret));
      } else {
        ObDASRowID inner_id;
        double inner_score = 0.0;
        if (OB_FAIL(inner_op_->advance_to(inner_target, inner_id, inner_score))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to advance inner op to next group", K(ret));
        } else {
          cmp = 0;
          if (OB_FAIL(compare_inner_group_with_pending(inner_id, cmp))) {
            LOG_WARN("failed to verify group after strict seek", K(ret));
          } else if (OB_UNLIKELY(cmp >= 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("strict seek did not move past pending group", K(ret), K(cmp),
                K(pending_group_doc_id_), K(pending_prefix_len),
                "inner_candidate", NestedRowIDDump(
                    search_ctx_, inner_op_->get_scope(), inner_id));
          } else if (OB_FAIL(emit_group_candidate(inner_id, next_id))) {
            LOG_WARN("failed to commit group after strict seek", K(ret));
          } else {
            score = inner_score;
            LOG_TRACE("[NESTED-DBG] skipped remaining rows for output group",
                K(pending_group_doc_id_), K(pending_prefix_len),
                "next_group", NestedRowIDDump(search_ctx_, scope_, next_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASNestedQueryOp::remember_for_next_skip(const ObDASRowID &emitted)
{
  int ret = OB_SUCCESS;
  clear_pending_group_skip();
  if (enable_group_skip_) {
    ObDatum group_doc_datum;
    ObDatum group_nk_datum;
    if (OB_FAIL(search_ctx_.get_datum_from_rowid(
            scope_, emitted, group_doc_datum, 0))) {
      LOG_WARN("failed to extract emitted doc id for pending group skip", K(ret));
    } else if (scope_ == DAS_SCOPE_NESTED) {
      const int64_t expected_parent_len =
          static_cast<int64_t>(nesting_depth_ - 1)
          * ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL;
      if (OB_FAIL(search_ctx_.get_datum_from_rowid(
              DAS_SCOPE_NESTED, emitted, group_nk_datum, 1))) {
        LOG_WARN("failed to extract emitted nested prefix for pending skip", K(ret));
      } else if (OB_UNLIKELY(group_nk_datum.len_ != expected_parent_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nested key length for parent-prefix skip",
            K(ret), K(group_nk_datum.len_), K(expected_parent_len), K(nesting_depth_));
      } else {
        MEMCPY(pending_group_prefix_, group_nk_datum.ptr_, group_nk_datum.len_);
        pending_group_doc_id_ = group_doc_datum.get_uint();
        has_pending_group_skip_ = true;
      }
    } else {
      pending_group_doc_id_ = group_doc_datum.get_uint();
      has_pending_group_skip_ = true;
    }
  }
  return ret;
}

void ObDASNestedQueryOp::reset_iteration_state()
{
  clear_pending_group_skip();
  last_emitted_rowid_.reset();
  has_emitted_ = false;
  current_allocator_is_a_ = true;
  dedup_allocator_a_.reset();
  dedup_allocator_b_.reset();
  advance_scratch_allocator_.reset();
}

int ObDASNestedQueryOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  // An external positioning request supersedes any outstanding internal group skip.
  // The returned group is remembered again below, independently of streaming dedup state.
  clear_pending_group_skip();
  LOG_TRACE("[NESTED-DBG] do_advance_to ENTER",
           K(scope_),
           "target", NestedRowIDDump(search_ctx_, scope_, target));
  if (OB_ISNULL(inner_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_op_ is null", K(ret));
  } else if (is_doc_id_only_) {
    double inner_score = 0.0;
    if (OB_FAIL(inner_op_->advance_to(target, curr_id, inner_score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance DOC_ID_ONLY scalar op", K(ret));
    } else if (OB_FAIL(remember_for_next_skip(curr_id))) {
      LOG_WARN("failed to remember advanced DOC_ID_ONLY doc", K(ret));
    } else {
      score = inner_score;
    }
  } else {
    // inner_target is only needed for the duration of this call; use the dedicated scratch
    // allocator and reset it at the top so it never accumulates across calls.
    advance_scratch_allocator_.reset();

    // Build inner_target by appending BYTES_PER_LEVEL bytes of 0x00 to the target's nested_key
    // (or, if parent scope is BASE, by constructing (doc_id, 0x00000000)).
    ObDASRowID inner_target;

    if (scope_ == DAS_SCOPE_BASE) {
      // Target is a plain uint64 doc_id. Inner target is COMPACT (doc_id, 0x00...0).
      ObDatum doc_id_datum;
      if (OB_FAIL(search_ctx_.get_datum_from_rowid(DAS_SCOPE_BASE, target, doc_id_datum, 0))) {
        LOG_WARN("failed to get doc_id datum from base-scope target", K(ret));
      } else if (OB_FAIL(build_inner_doc_target(
                     doc_id_datum.get_uint(), inner_target, advance_scratch_allocator_))) {
        LOG_WARN("failed to build inner target from base-scope target", K(ret));
      }
    } else {
      // Target is COMPACT (doc_id, nk). Inner target is COMPACT (doc_id, nk + BYTES_PER_LEVEL bytes of 0x00).
      ObDatum doc_id_datum;
      ObDatum nk_datum;
      if (OB_FAIL(search_ctx_.get_datum_from_rowid(scope_, target, doc_id_datum, 0))) {
        LOG_WARN("failed to get doc_id datum from nested-scope target", K(ret));
      } else if (OB_FAIL(search_ctx_.get_datum_from_rowid(scope_, target, nk_datum, 1))) {
        LOG_WARN("failed to get nk datum from nested-scope target", K(ret));
      } else {
        // Build extended nk = nk + BYTES_PER_LEVEL zero bytes
        const int64_t ext_nk_len = nk_datum.len_ + ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL;
        char *ext_nk_buf = nullptr;
        if (OB_ISNULL(ext_nk_buf = static_cast<char *>(advance_scratch_allocator_.alloc(ext_nk_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate extended nk buffer", K(ret), K(ext_nk_len));
        } else {
          MEMCPY(ext_nk_buf, nk_datum.ptr_, nk_datum.len_);
          MEMSET(ext_nk_buf + nk_datum.len_, 0, ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL);
          ObDatum ext_nk_datum;
          ext_nk_datum.ptr_ = ext_nk_buf;
          ext_nk_datum.pack_ = static_cast<uint32_t>(ext_nk_len);
          if (OB_FAIL(build_nested_rowid(
                  doc_id_datum, ext_nk_datum, inner_target,
                  advance_scratch_allocator_))) {
            LOG_WARN("failed to build inner target from nested-scope target", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObDASRowID inner_id;
      double inner_score = 0.0;
      if (OB_FAIL(inner_op_->advance_to(inner_target, inner_id, inner_score))) {
        // OB_ITER_END propagates
      } else {
        // advance_to() must return the first emitted rowid >= target even when it equals
        // the last rowid produced by a previous lookahead. Streaming dedup is only for
        // next_rowid(); applying it here would make ReqExclOp miss exclusions.
        if (OB_FAIL(build_truncated_rowid(inner_id, curr_id, advance_scratch_allocator_))) {
          LOG_WARN("failed to build truncated rowid after advance", K(ret));
        } else if (OB_FAIL(remember_for_next_skip(curr_id))) {
          LOG_WARN("failed to remember advanced group for next skip", K(ret));
        } else {
          score = inner_score;
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
