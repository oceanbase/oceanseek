/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_SCAN_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_SCAN_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "share/logical_table/ob_logical_table_define.h"

namespace oceanbase
{
namespace sql
{

// Runtime projection of the expr vectors populated by a scalar RowID producer.
// A physical nested index can write [doc_id, nested_key] while its BASE-scope
// consumer only needs doc_id. Conversely, a NESTED-scope consumer must retain
// both columns, and different search indexes can use different expr objects.
// Capture that contract once when the producer is initialized instead of making
// downstream operators infer it from scope, RowID type, or CTDef provenance.
class ObDASRowIDOutputExprs
{
public:
  ObDASRowIDOutputExprs() : exprs_() {}
  ~ObDASRowIDOutputExprs() = default;

  int init(const common::ObIArray<ObExpr *> &producer_exprs,
           const int64_t output_expr_count);

  OB_INLINE const common::ObIArray<ObExpr *> &get_exprs() const { return exprs_; }
  OB_INLINE ObExpr *get_doc_id_expr() const
  { return exprs_.empty() ? nullptr : exprs_.at(0); }

private:
  common::ObSEArray<ObExpr *, 2> exprs_;
};

class ObDASScalarScanOpParam : public ObIDASSearchOpParam
{
public:
  ObDASScalarScanOpParam(const ObDASScalarScanCtDef *scan_ctdef,
                         ObDASScalarScanRtDef *scan_rtdef)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_SCALAR_SCAN), scan_ctdef_(scan_ctdef), scan_rtdef_(scan_rtdef) {}
  ~ObDASScalarScanOpParam() {}
  OB_INLINE const ObDASScalarScanCtDef *get_scan_ctdef() const { return scan_ctdef_; }
  OB_INLINE ObDASScalarScanRtDef *get_scan_rtdef() const { return scan_rtdef_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
private:
  const ObDASScalarScanCtDef *scan_ctdef_;
  ObDASScalarScanRtDef *scan_rtdef_;
};
 
class ObDASScalarScanOp : public ObIDASSearchOp
{
public:
  ObDASScalarScanOp(ObDASSearchCtx &search_ctx)
    : ObIDASSearchOp(search_ctx),
      scan_param_(),
      scalar_ctdef_(nullptr),
      scalar_rtdef_(nullptr),
      result_(nullptr),
      tsc_service_(nullptr),
      tablet_id_(),
      rowid_output_exprs_()
  { }
  virtual int get_next_rows(int64_t &count, const int64_t max_batch_size)
  { return next_rowids(max_batch_size, count); }
  const ObDASScalarScanCtDef *get_scalar_ctdef() const { return scalar_ctdef_; }
  const ObDASScalarScanRtDef *get_scalar_scan_rtdef() const { return scalar_rtdef_; }
  const storage::ObTableScanParam &get_scan_param() const { return scan_param_; }
  // Expr vectors this producer populates for its logical RowID output. The
  // pointers belong to this concrete child, not to a shared scope anchor.
  OB_INLINE const common::ObIArray<ObExpr *> *get_rowid_output_exprs() const override
  { return &rowid_output_exprs_.get_exprs(); }
  OB_INLINE ObExpr *get_doc_id_output_expr() const
  { return rowid_output_exprs_.get_doc_id_expr(); }
  static int prepare_logical_table_kv_scan_ranges(
      const ObLogicalTableSearchCtx &lt_ctx,
      const ObDASScalarScanCtDef *scalar_ctdef,
      const common::ObIArray<common::ObNewRange> &logical_ranges,
      common::ObIAllocator &alloc,
      common::ObIArray<common::ObNewRange> &physical_ranges);
  static int fill_logical_rowid_exprs(ObDASSearchCtx &search_ctx,
                                      const ObDASScalarScanCtDef *scalar_ctdef,
                                      int64_t count);
  static int resolve_scalar_tablet_id(ObIDASSearchOp &search_op,
                                      ObDASSearchCtx &search_ctx,
                                      const ObDASScalarScanCtDef *scalar_ctdef,
                                      common::ObTabletID &tablet_id);
  // After storage batch read: project, eval logical_residual_filters_, fill rowid from kv_key.
  // scalar_rtdef must be non-null and have lt_scan_rt_ allocated (via ensure_lt_scan_rt_allocated)
  // when scalar_ctdef->is_logical_table() and logical_output_exprs_ is non-empty.
  static int process_logical_table_storage_batch(ObDASSearchCtx &search_ctx,
                                                 const ObDASScalarScanCtDef *scalar_ctdef,
                                                 ObDASScalarScanRtDef *scalar_rtdef,
                                                 int64_t &storage_count);

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override { return OB_NOT_SUPPORTED; };
  int do_next_rowid(ObDASRowID &next_id, double &score) override { return OB_NOT_SUPPORTED; };
  int do_next_rowids(int64_t capacity, int64_t &count) override;

private:
  int prepare_scan_ranges(const ObDASScalarScanRtDef *rtdef);
  int fill_logical_rowid_exprs_(int64_t count);
  // When logical_residual_filters_ compacted a whole storage batch to zero rows,
  // keep pulling the next storage batch so later batches are not silently lost.
  int repull_logical_table_batches_(int64_t &count, const int64_t max_batch_size);

private:
  storage::ObTableScanParam scan_param_;

protected:
  int init_rowid_output_exprs(const common::ObIArray<ObExpr *> &producer_exprs);
  const ObDASScalarScanCtDef *scalar_ctdef_;
  ObDASScalarScanRtDef *scalar_rtdef_;

private:
  common::ObNewRowIterator *result_;
  storage::ObAccessService *tsc_service_;
  common::ObTabletID tablet_id_;
  ObDASRowIDOutputExprs rowid_output_exprs_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_SCAN_OP_H_
