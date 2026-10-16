/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SCAN_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SCAN_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/ob_das_scan_op.h"
namespace oceanbase
{
namespace common {
class ObITabletScan;
}

namespace storage {
class ObTableScanParam;
}

namespace sql
{

class ObDASScanCtDef;
struct ObDASScalarScanCtDef;

// DASScanIter is a wrapper class for storage iter, it doesn't require eval_ctx or exprs like other iters.
struct ObDASScanIterParam : public ObDASIterParam
{
public:
  ObDASScanIterParam()
    : ObDASIterParam(ObDASIterType::DAS_ITER_SCAN),
      scan_ctdef_(nullptr),
      scalar_scan_ctdef_(nullptr),
      use_scalar_ctdef_(false),
      self_managed_logical_lt_ctx_(false),
      namespace_id_(share::OB_INVALID_NAMESPACE_ID),
      ltable_id_(share::OB_INVALID_LOGICAL_TABLE_ID),
      logical_ivf_vec_project_dim_(-1)
  {}
  const ObDASScanCtDef *scan_ctdef_;
  const ObDASScalarScanCtDef *scalar_scan_ctdef_;
  bool use_scalar_ctdef_;
  /// When true, ObDASScanIter reads logical-table identity (ns/lt) and kv exprs
  /// directly from scan_iter_param + scan_ctdef_/scalar_scan_ctdef_, bypassing the
  /// op-level lt_ctx_. Used by logical Agent DB IVF auxiliary scans (centroid /
  /// cid_vec / rowkey_cid / com_aux / special_aux as applicable) and hybrid FTS aux.
  bool self_managed_logical_lt_ctx_;
  /// Runtime @namespace_id for logical-table self-managed scans (hybrid FTS / plain MATCH aux).
  uint64_t namespace_id_;
  /// Runtime @ltable_id; mapped to lt_rt_for_proj_.logical_table_id_ in inner_init.
  uint64_t ltable_id_;
  /// When > 0, logical IVF cid_vec project needs this dim to materialize
  /// vector column as dim*sizeof(float) continuous binary in project_logical_columns.
  int64_t logical_ivf_vec_project_dim_;
  virtual bool is_valid() const override
  {
    const bool ctdef_valid = (!use_scalar_ctdef_ && nullptr != scan_ctdef_)
                          || (use_scalar_ctdef_ && nullptr != scalar_scan_ctdef_);
    return ctdef_valid && ObDASIterParam::is_valid();
  }
  const ExprFixedArray &get_result_output() const;
  common::ObTableID get_ref_table_id() const;
  bool is_external_table() const;
};

class ObDASScanIter : public ObDASIter
{
public:
  ObDASScanIter()
    : ObDASIter(ObDASIterType::DAS_ITER_SCAN),
      tsc_service_(nullptr),
      result_(nullptr),
      scan_param_(nullptr),
      scan_ctdef_(nullptr),
      scalar_scan_ctdef_(nullptr),
      skip_vectorized_print_(false),
      try_check_tick_(0),
      try_check_tick_rows_(0),
      vec_start_scan_ts_us_(-1),
      vec_pre_filtering_timeout_us_(-1),
      owned_lt_scan_ctdef_(),
      lt_rt_for_proj_(),
      owned_lt_scan_rt_(nullptr)
  {}
  virtual ~ObDASScanIter() {}
  common::ObNewRowIterator *&get_output_result_iter() { return result_; }

  void set_scan_param(storage::ObTableScanParam &scan_param) { scan_param_ = &scan_param; }
  storage::ObTableScanParam &get_scan_param() { return *scan_param_; }
  OB_INLINE void set_skip_vectorized_print() { skip_vectorized_print_ = true; }
  OB_INLINE bool is_skip_vectorized_print() { return skip_vectorized_print_; }
  /// Initialize owned logical-table fields from rtdef/ctdef directly, bypassing op-level lt_ctx_.
  /// Called by ObDASScanOp::open_op and inject_logical_lookup_lt_ctx_ (replaces former set_lt_ctx).
  void init_lt_from_defs(const ObLogicalTableRtDef &lt_rt,
                         const ObDASScanCtDef *scan_ctdef_src,
                         const ObDASScalarScanCtDef *scalar_ctdef_src,
                         const ObLogicalTableScanRtDef *lt_scan_rt_src,
                         ObExpr *kv_key_override = nullptr,
                         ObExpr *kv_value_override = nullptr);
  // When domain-merge builds physical ROWID_CID ranges itself, skip
  // try_session_lt_range_rewrite_ so ranges are not treated as logical again.
  int mark_direct_phys_ranges_built(const bool built = true);

  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual int advance_scan() override;
  virtual void clear_evaluated_flag() override;

  virtual int set_scan_rowkey(ObEvalCtx *eval_ctx,
                              const ObIArray<ObExpr *> &rowkey_exprs,
                              const ObDASScanCtDef *lookup_ctdef,
                              ObIAllocator *alloc,
                              int64_t group_id) override;
  OB_INLINE void set_pre_filtering_timeout(int64_t vec_pre_filtering_timeout) { vec_pre_filtering_timeout_us_ = vec_pre_filtering_timeout; }
  OB_INLINE bool is_vec_pre_filtering_timeout_set() { return vec_pre_filtering_timeout_us_ > 0; }
protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  int try_check_vec_pre_filter_status(const int64_t row_count = 1);
  int check_vec_pre_filter_status();
  // Attempt the logical-table session-level key-range rewrite.
  // Dispatches between scan_ctdef_ / scalar_scan_ctdef_ (mutually exclusive).
  // Callers (do_table_scan / rescan / advance_scan) must still issue the
  // underlying storage scan after this returns OB_SUCCESS.
  int try_session_lt_range_rewrite_();
  // Shared LT projection + residual-filter evaluation for the current batch of
  // count rows (count == 1 for the single-row variant); all_filtered is set
  // when prefix/residual filters dropped every row (count reset to 0).
  int lt_project_filter_current_(int64_t &count, int in_ret, bool &all_filtered);
  int lt_project_filter_rows_with_repull_(int64_t &count, int64_t capacity, int in_ret);
  int lt_project_filter_row_with_repull_(int in_ret);

private:
  bool need_filter_mixed_primary_rows_() const;
  int filter_mixed_primary_rows_(int64_t &row_count);
  int compact_physical_exprs_(const ObBitVector &skip, int64_t &row_count);
  int compact_physical_expr_(ObExpr &expr,
                             const ObBitVector &skip,
                             const int64_t row_count);
  int compact_continuous_vector_(common::ObIVector &vector,
                                 const ObBitVector &skip,
                                 const int64_t row_count);
  static int append_unique_physical_expr_(
      ObExpr *expr,
      common::ObIArray<ObExpr *> &exprs);

  static const uint64_t CHECK_STATUS_ROWS = 500;
  static const uint64_t CHECK_STATUS_TRY_TIMES = 50;

  common::ObITabletScan *tsc_service_;
  common::ObNewRowIterator *result_;
  // must ensure the lifecycle of scan param is longer than scan iter.
  storage::ObTableScanParam *scan_param_;
  const ObDASScanCtDef *scan_ctdef_;     // stored at inner_init for logical table decode
  /// Non-null only when use_scalar_ctdef_=true at param time. Used by FTS token op
  /// path so scalar scans can also do logical KV range rewrite + JSON projection.
  const ObDASScalarScanCtDef *scalar_scan_ctdef_;
  bool skip_vectorized_print_;
  uint64_t try_check_tick_; // for check vector prefilter search status by times, default 0
  uint64_t try_check_tick_rows_; // for check vector prefilter search status by rows, default 0
  int64_t vec_start_scan_ts_us_; // the start timestamp of this scan for vector prefilter search, default -1
  int64_t vec_pre_filtering_timeout_us_; // the pre-filtering timeout of vector prefilter search , default -1
  /// Owned compile-time descriptor built from scan_ctdef_->lt_ctx_ or scalar_scan_ctdef_->lt_ctx_
  /// at inner_init time, with kv_key/value_expr_ and ivf_vec_project_dim_ overridden from lt_ctx_.
  /// Used with the new-signature project_logical_columns path.
  ObLogicalTableScanCtDef owned_lt_scan_ctdef_;
  /// Lightweight runtime identity populated from lt_ctx_ at inner_init.
  ObLogicalTableRtDef lt_rt_for_proj_;
  /// Owned heavy runtime state (path_cache_ inside) for new-signature project_logical_columns.
  /// Lazy-allocated on logical-table scan paths only; nullptr for plain table scans.
  ObLogicalTableScanRtDef *owned_lt_scan_rt_;
};


}  // namespace sql
}  // namespace oceanbase



#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SCAN_ITER_H_ */
