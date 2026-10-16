/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_PRIMARY_ROR_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_PRIMARY_ROR_OP_H_

#include "sql/das/search/ob_das_scalar_ror_op.h"

namespace oceanbase
{
namespace sql
{

class ObDASScalarPrimaryROROpParam : public ObIDASSearchOpParam
{
public:
  ObDASScalarPrimaryROROpParam(const ObDASScalarScanCtDef *scan_ctdef,
                               ObDASScalarScanRtDef *scan_rtdef)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_SCALAR_PRIMARY_ROR), scan_ctdef_(scan_ctdef), scan_rtdef_(scan_rtdef), is_probe_mode_(false) {}
  ~ObDASScalarPrimaryROROpParam() {}
  OB_INLINE const ObDASScalarScanCtDef *get_scan_ctdef() const { return scan_ctdef_; }
  OB_INLINE ObDASScalarScanRtDef *get_scan_rtdef() const { return scan_rtdef_; }
  // probe is supported and next/advance_to are not supported when in probe mode.
  OB_INLINE void set_is_probe_mode(bool is_probe_mode) { is_probe_mode_ = is_probe_mode; }
  OB_INLINE bool get_is_probe_mode() const { return is_probe_mode_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
private:
  const ObDASScalarScanCtDef *scan_ctdef_;
  ObDASScalarScanRtDef *scan_rtdef_;
  bool is_probe_mode_;
};

class ObDASScalarPrimaryROROp : public ObDASScalarROROp
{
public:
  ObDASScalarPrimaryROROp(ObDASSearchCtx &search_ctx)
    : ObDASScalarROROp(search_ctx),
      get_param_(),
      get_result_(nullptr),
      is_probe_mode_(false)
  { }

protected:
  int do_open() override;
  int do_rescan() override;
  int do_close() override;
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_shallow(const ObDASRowID &target,
                         const bool inclusive,
                         const MaxScoreTuple *&max_score_tuple) override;
  virtual bool is_probe_mode() const override { return is_probe_mode_; }
  int do_probe(const ObDASRowID &target, bool &hit) override;

  int init_get_pd_op();
  int point_get(const ObDASRowID &target, bool &found);
  virtual int advance_skip_scan(const ObDASRowID &target) override;

  // Probe-mode extension points. The default implementations target a physical primary
  // table; ObDASScalarLogicPrimaryROROp overrides them for logical (KV) tables so the
  // logical-table special handling lives entirely in the subclass.
  //   prepare_probe_ranges: build get_param_.key_ranges_ during do_open().
  //   locate_probe_range:   narrow get_param_.key_ranges_ to the target rowid before rescan.
  //   fetch_probe_row:      read one probe result and decide whether the target hits.
  //   release_probe_ranges: free key-range memory during do_close().
  virtual int prepare_probe_ranges(common::ObIAllocator &allocator);
  virtual int locate_probe_range(const ObDASRowID &target);
  virtual int fetch_probe_row(bool &found);
  virtual void release_probe_ranges(common::ObIAllocator &allocator);

protected:
  storage::ObTableScanParam get_param_;
  common::ObNewRowIterator *get_result_;

private:
  bool is_probe_mode_;

private:
  union {
    // Independent ObPushdownOperator for get_param_ to avoid state sharing with scan_param_
    ObPushdownOperator get_pd_expr_op_;
  };
};

// Logical-table variant of the primary ROR probe. The probe physically hits the KV data
// table, so all logical-specific range encoding, redirection and residual filtering is
// isolated here via the ObDASScalarPrimaryROROp probe extension points.
class ObDASScalarLogicPrimaryROROp : public ObDASScalarPrimaryROROp
{
public:
  ObDASScalarLogicPrimaryROROp(ObDASSearchCtx &search_ctx)
    : ObDASScalarPrimaryROROp(search_ctx),
      probe_range_alloc_(ObMemAttr(MTL_ID(), "LTProbeRange"))
  { }

protected:
  int prepare_probe_ranges(common::ObIAllocator &allocator) override;
  int locate_probe_range(const ObDASRowID &target) override;
  int fetch_probe_row(bool &found) override;
  void release_probe_ranges(common::ObIAllocator &allocator) override;

private:
  // Per-probe KV lookup range memory for logical tables; reuse() each point_get so
  // range memory stays O(1) instead of accumulating on the statement allocator.
  common::ObArenaAllocator probe_range_alloc_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_PRIMARY_ROR_OP_H_