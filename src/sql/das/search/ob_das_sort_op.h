/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SORT_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SORT_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_scalar_scan_op.h"
#include "sql/engine/sort/ob_sort_op_impl.h"

namespace oceanbase
{
namespace sql
{

class ObDASSortOpParam : public ObIDASSearchOpParam
{
public:
  ObDASSortOpParam(ObIDASSearchOp *child,
                   const ObDASScalarScanCtDef *scalar_ctdef,
                   ObDASScalarScanRtDef *scalar_rtdef,
                   bool unique_sort = false)
  : ObIDASSearchOpParam(DAS_SEARCH_OP_SORT),
    child_(child),
    scalar_ctdef_(scalar_ctdef),
    scalar_rtdef_(scalar_rtdef),
    unique_sort_(unique_sort)
  { }
  virtual ~ObDASSortOpParam() {}
  OB_INLINE ObIDASSearchOp *get_child() const { return child_; }
  OB_INLINE const ObDASScalarScanCtDef *get_scalar_ctdef() const { return scalar_ctdef_; }
  OB_INLINE ObDASScalarScanRtDef *get_scalar_rtdef() const { return scalar_rtdef_; }
  OB_INLINE bool is_unique_sort() const { return unique_sort_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;

private:
  ObIDASSearchOp *child_;
  const ObDASScalarScanCtDef *scalar_ctdef_;
  ObDASScalarScanRtDef *scalar_rtdef_;
  // When true, adjacent equal rows (by all rowid_exprs columns) are deduplicated during output.
  // Used by NESTED child-domain must_not to emit unique (doc_id, nk) pairs.
  bool unique_sort_;
};

class ObDASSortOp : public ObIDASSearchOp
{
public:
  ObDASSortOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    sort_finished_(false),
    unique_sort_(false),
    input_op_(nullptr),
    input_rowid_exprs_(nullptr),
    rowid_store_(nullptr),
    rowid_store_iter_(),
    sort_impl_(nullptr),
    sort_collations_(ctx_allocator()),
    sort_cmp_funcs_(ctx_allocator()),
    sort_memctx_(),
    nk_truncate_depth_(0),
    boost_weight_(0.0),
    is_first_nk_(true),
    nested_input_end_(false),
    curr_nk_buf_(nullptr)
  { curr_doc_id_.set_min(); }
  virtual ~ObDASSortOp() {}

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_rescan_segment(const common::ObNewVersionRange &range) override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_to(const ObDASNestedKey &target, ObDASNestedKey &curr_nk, double &score) override;
  int do_next_rowid(ObDASNestedKey &next_nk, double &score) override;
  int next_impl(ObDASNestedKey &next_nk, double &score);
  int do_advance_shallow(const ObDASRowID &target, const bool inclusive, const MaxScoreTuple *&max_score_tuple) override;
  int do_calc_max_score(double &threshold) override;

private:
  int do_sort();
  int next_sorted_rowid(ObDASRowID &next_id, double &score);
  int advance_sorted_rowid(const ObDASRowID &target, ObDASRowID &curr_id, double &score);
  void reset_rowid_cursor();
  int copy_nested_key(ObDASNestedKey &key);
  // Sort must read the vectors written by its scalar child. They are normally
  // identical to this op's scoped RowID exprs, except for BASE DOC_ID_ONLY where
  // the child writes search-index doc_id while the outer fusion BASE expr
  // belongs to the lookup table.
  OB_INLINE const common::ObIArray<ObExpr *> &get_sort_rowid_exprs() const
  { return *input_rowid_exprs_; }

private:
  bool sort_finished_;
  // When true, ObUniqueSortImpl is used so equal-sort-key rows (full rowid
  // [doc_id, nested_key]) are deduplicated inside the sort. See do_init.
  bool unique_sort_;
  ObIDASSearchOp *input_op_;
  const common::ObIArray<ObExpr *> *input_rowid_exprs_;
  RowIDStore *rowid_store_;
  RowIDStore::Iterator rowid_store_iter_;
  // TODO: use ObSortVecOpImpl instead of ObSortOpImpl
  // Points to an ObUniqueSortImpl when unique_sort_, else an ObSortOpImpl.
  ObSortOpImpl *sort_impl_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funcs_;
  lib::MemoryContext sort_memctx_;
  // When > 0, nested_key datums are truncated to nk_truncate_depth_ * BYTES_PER_LEVEL bytes in
  // do_sort() before add_batch. This collapses deeper-level nested elements to the
  // current depth's identity, ensuring ObUniqueSortImpl deduplicates correctly.
  int64_t nk_truncate_depth_;
  double boost_weight_;
  bool is_first_nk_;
  ObDASNestedKey curr_nk_;
  ObDASRowID curr_doc_id_;
  bool nested_input_end_;
  char *curr_nk_buf_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SORT_OP_H_
