/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_ROR_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_ROR_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/search/ob_das_scalar_scan_op.h"

namespace oceanbase
{
namespace sql
{

class ObDASScalarROROp : public ObIDASSearchOp
{
public:
  ObDASScalarROROp(ObDASSearchCtx &search_ctx)
    : ObIDASSearchOp(search_ctx),
      scan_param_(),
      scan_range_allocator_(common::ObMemAttr(MTL_ID(), "DASRORRange")),
      scalar_ctdef_(nullptr),
      scalar_rtdef_(nullptr),
      result_(nullptr),
      tsc_service_(nullptr),
      tablet_id_(),
      rowid_store_(nullptr),
      rowid_store_iter_(),
      rowid_output_exprs_()
  { }
  virtual ~ObDASScalarROROp() {}

protected:
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_shallow(const ObDASRowID &target, const bool inclusive, const MaxScoreTuple *&max_score_tuple) override;
  int do_calc_max_score(double &threshold) override;
  int64_t get_self_estimated_work_row_count() const override
  {
    return OB_ISNULL(scalar_rtdef_)
        ? -1
        : scalar_rtdef_->get_estimated_work_row_count();
  }
  virtual int advance_skip_scan(const ObDASRowID &target) = 0;
  virtual int prepare_scan_ranges(const ObDASScalarScanRtDef *rtdef);
  // Fetch one storage batch and run logical-table post-processing.
  // Returns OB_SUCCESS with storage_count > 0, or OB_ITER_END when storage is exhausted.
  // Batches compacted to zero by logical residual filters are skipped internally.
  int fetch_and_process_storage_batch_(int64_t &storage_count);
  int narrow_scan_ranges_by_docid_range();

  // RowID projection populated by this concrete ROR producer. A BASE DOC_ID_ONLY
  // scan can project doc_id from a physical [doc_id, nested_key] rowkey, while
  // NESTED keeps both child-local exprs.
  OB_INLINE const common::ObIArray<ObExpr*> *get_rowid_output_exprs() const override
  { return &rowid_output_exprs_.get_exprs(); }

protected:
  storage::ObTableScanParam scan_param_;
  // Mutable DocID slices belong to this operator, not the rtdef shared by Groups.
  common::ObArenaAllocator scan_range_allocator_;
  const ObDASScalarScanCtDef *scalar_ctdef_;
  ObDASScalarScanRtDef *scalar_rtdef_;
  common::ObNewRowIterator *result_;
  storage::ObAccessService *tsc_service_;
  common::ObTabletID tablet_id_;

  RowIDStore *rowid_store_;
  RowIDStore::Iterator rowid_store_iter_;
  ObDASRowIDOutputExprs rowid_output_exprs_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_ROR_OP_H_
