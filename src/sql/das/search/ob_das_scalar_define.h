/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SCALAR_DEFINE_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SCALAR_DEFINE_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "storage/tx_storage/ob_access_service.h"
#include "share/logical_table/ob_logical_table_define.h"
#include "sql/das/ob_das_define.h"

namespace oceanbase
{
namespace sql
{

struct ObDASScalarCtDef : ObIDASSearchCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASScalarCtDef(common::ObIAllocator &alloc)
    : ObIDASSearchCtDef(alloc, DAS_OP_SCALAR_QUERY),
      boost_(nullptr),
      has_index_scan_(false),
      has_main_scan_(false),
      primary_get_ratio_(0)
  { }
  virtual ~ObDASScalarCtDef() {}

  OB_INLINE bool has_main_scan() const { return has_main_scan_; }
  OB_INLINE bool has_index_scan() const { return has_index_scan_; }
  OB_INLINE void set_has_main_scan(bool has_main_scan) { has_main_scan_ = has_main_scan; }
  OB_INLINE void set_has_index_scan(bool has_index_scan) { has_index_scan_ = has_index_scan; }
  OB_INLINE int64_t get_primary_get_ratio() const override { return primary_get_ratio_; }
  OB_INLINE void set_primary_get_ratio(int64_t primary_get_ratio) { primary_get_ratio_ = primary_get_ratio; }

  const ObDASScalarScanCtDef* get_index_scan_ctdef() const;
  const ObDASScalarScanCtDef* get_main_scan_ctdef() const;
  int eval_boost(ObEvalCtx *eval_ctx, double &boost_weight) const;

  ObExpr *boost_;

private:
  bool has_index_scan_;
  bool has_main_scan_;
  // Primary-get optimization ratio for this scalar query; see ObDSLBaseSearchOption.
  // 0 means the optimization is disabled and the index scan path is always used
  // (the original behavior).
  int64_t primary_get_ratio_;
};

struct ObDASScalarRtDef : ObIDASSearchRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASScalarRtDef()
    : ObIDASSearchRtDef(DAS_OP_SCALAR_QUERY)
  { }
  virtual ~ObDASScalarRtDef() {}

  virtual int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override;
  virtual int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op) override;
  // Generate the primary-table point probe selected by a specialized planner model.
  // This expresses a physical-path decision directly instead of distorting cardinality cost.
  int generate_selected_primary_probe_op(
      ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  bool can_build_direct_rowid_drain(const ObDASSearchCtx &search_ctx) const;
  int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx,
      bool allow_direct_drain, ObIDASSearchOp *&op);
  const ObDASScalarCtDef* get_ctdef() const { return static_cast<const ObDASScalarCtDef *>(ctdef_); }
  ObDASScalarScanRtDef* get_index_scan_rtdef() const;
  ObDASScalarScanRtDef* get_main_scan_rtdef() const;
private:
  int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx,
      bool allow_direct_drain, bool force_primary_probe, ObIDASSearchOp *&op);
};

struct ObDASScalarScanCtDef : ObIDASSearchCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASScalarScanCtDef(common::ObIAllocator &alloc)
    : ObIDASSearchCtDef(alloc, DAS_OP_SCALAR_SCAN_QUERY),
      ref_table_id_(common::OB_INVALID_ID),
      access_column_ids_(alloc),
      schema_version_(-1),
      table_param_(alloc),
      pd_expr_spec_(alloc),
      result_output_(alloc),
      rowkey_exprs_(alloc),
      table_scan_opt_(),
      pre_query_range_(alloc),
      pre_range_graph_(alloc),
      flags_(0),
      logical_output_exprs_(alloc),
      lt_ctx_(),
      logical_residual_filters_(alloc),
      column_idx_for_full_subtree_(0),
      nesting_depth_for_full_subtree_(0)
  { }
  virtual ~ObDASScalarScanCtDef() {}

  const ObQueryRangeProvider& get_query_range_provider() const
  {
    return is_new_query_range_ ? static_cast<const ObQueryRangeProvider&>(pre_range_graph_)
                               : static_cast<const ObQueryRangeProvider&>(pre_query_range_);
  }
  bool is_async_ror_scan() const
  {
    return is_async_index_ && (is_index_term_point_scan_ || is_rowkey_order_scan_);
  }

  INHERIT_TO_STRING_KV("ObIDASSearchCtDef", ObIDASSearchCtDef,
                       K_(ref_table_id),
                       K_(access_column_ids),
                       K_(schema_version),
                       K_(table_param),
                       K_(pd_expr_spec),
                       K_(result_output),
                       K_(rowkey_exprs),
                       K_(table_scan_opt),
                       K_(pre_query_range),
                       K_(pre_range_graph),
                       K_(flags),
                       "logical_output_cnt", logical_output_exprs_.count(),
                       K_(lt_ctx),
                       K_(nested_path_for_full_subtree));

  common::ObTableID ref_table_id_;
  UIntFixedArray access_column_ids_;
  int64_t schema_version_;
  share::schema::ObTableParam table_param_;
  ObPushdownExprSpec pd_expr_spec_;
  sql::ExprFixedArray result_output_;
  sql::ExprFixedArray rowkey_exprs_;
  ObTableScanOption table_scan_opt_;
  ObQueryRange pre_query_range_;
  ObPreRangeGraph pre_range_graph_;
  union {
    uint64_t flags_;
    struct {
      uint64_t is_primary_table_scan_        : 1;
      uint64_t is_new_query_range_           : 1;
      uint64_t enable_new_false_range_       : 1;
      uint64_t is_get_                       : 1;
      uint64_t is_rowkey_order_scan_         : 1;
      uint64_t need_rowkey_order_            : 1;
      uint64_t is_search_index_              : 1;
      // Logical provenance: this leaf scalar-scan is a descendant of an
      // ObDASNestedQueryCtDef. Runtime Sort/Bitmap routing must use the actual
      // created scope and RowID type instead: a safely flattened direct scalar
      // runs as BASE/uint64, while a subtree retaining nested-element identity
      // runs as NESTED/COMPACT.
      uint64_t is_nested_scope_              : 1;
      // Set on a synthesized full-subtree scan ctdef injected as the "required"
      // source when a nested() wraps a bool with only must_not clauses.
      // At runtime prepare_hybrid_search_scan_range uses a full MIN..MAX range
      // instead of extracting from pre_query_range_, so the sort op emits every
      // (doc_id, nested_key) pair for the subtree before must_not exclusion.
      uint64_t is_full_subtree_scan_for_nested_ : 1;
      // When set on a synthesized full-subtree scan, the wrapping sort op
      // should be constructed with unique_sort=true to deduplicate rows.
      uint64_t is_wrap_with_unique_sort_     : 1;
      uint64_t is_async_index_               : 1;  // async scalar filter index: drive via ObDASAsyncScalarRorOp
      // Set on the CG-synthesized data-table universe scan injected as must[0]
      // for parent-scope bool{must_not:[nested{...}]} plans. Signals runtime
      // prepare_hybrid_search_scan_range to push a whole-range (MIN..MAX) into
      // key_ranges_ without relying on pre_query_range_/pre_range_graph_ (op
      // has no attached query range for the hybrid_search table-scan node).
      uint64_t is_data_table_universe_scan_  : 1;
      // Each range fixes the complete term prefix; multiple ranges are not
      // necessarily globally ordered by RowID.
      uint64_t is_index_term_point_scan_     : 1;
      uint64_t reserved_                     : 51;
    };
  };
  sql::ExprFixedArray logical_output_exprs_;
  ObLogicalTableScanCtDef lt_ctx_;
  sql::ExprFixedArray logical_residual_filters_;
  // Nested path string for full-subtree scan synthesis.
  // Only meaningful when is_full_subtree_scan_for_nested_ == 1.
  common::ObString nested_path_for_full_subtree_;
  // Column index value in the search index rowkey for the owning JSON column.
  // Only meaningful when is_full_subtree_scan_for_nested_ == 1.
  int64_t column_idx_for_full_subtree_;
  // Nesting depth of the enclosing nested query (1 = single level, 2 = double, ...).
  // Used by ObDASSortOp to truncate nested_key to depth*BYTES_PER_LEVEL bytes before sort/dedup,
  // so that deeper-level rows are collapsed to the current nested element identity.
  // Only meaningful when is_full_subtree_scan_for_nested_ == 1.
  int32_t nesting_depth_for_full_subtree_;
};

struct ObDASScalarScanRtDef : ObIDASSearchRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASScalarScanRtDef()
    : ObIDASSearchRtDef(DAS_OP_SCALAR_SCAN_QUERY),
      p_row2exprs_projector_(nullptr),
      p_pd_expr_op_(nullptr),
      tenant_schema_version_(-1),
      timeout_ts_(-1),
      tx_lock_timeout_(-1),
      sql_mode_(SMO_DEFAULT),
      scan_flag_(),
      stmt_allocator_("StmtScanAlloc"),
      scan_allocator_("TableScanAlloc"),
      key_ranges_(),
      really_need_rowkey_order_(true),
      lt_rt_(),
      lt_scan_rt_(nullptr),
      estimated_work_row_count_(-1),
      boost_weight_(0.0)
  { }
  virtual ~ObDASScalarScanRtDef()
  {
    if (lt_scan_rt_ != nullptr) {
      lt_scan_rt_->~ObLogicalTableScanRtDef();
      lt_scan_rt_ = nullptr;
    }
  }

  int init_pd_op(ObExecContext &exec_ctx, const ObDASScalarScanCtDef &scalar_ctdef);
  virtual int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override;
  virtual int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op) override;
  bool can_build_direct_rowid_drain(const ObDASSearchCtx &search_ctx) const;
  int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx,
      bool allow_direct_drain, ObIDASSearchOp *&op);
  int create_async_ror_op(
      ObDASSearchCtx &search_ctx,
      const int64_t range_idx,
      const uint64_t index_table_id,
      const int64_t group_slot,
      const bool direct_bind,
      ObIDASSearchOp *&op);
  bool can_build_streaming_group_filter(const ObDASSearchCtx &search_ctx) const;
  int generate_streaming_group_filter_op(
      ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  const ObDASScalarScanCtDef* get_ctdef() const { return static_cast<const ObDASScalarScanCtDef *>(ctdef_); }

  INHERIT_TO_STRING_KV("ObIDASSearchRtDef", ObIDASSearchRtDef,
                        K_(tenant_schema_version),
                        K_(timeout_ts),
                        K_(tx_lock_timeout),
                        K_(sql_mode),
                        K_(scan_flag),
                        K_(key_ranges),
                        K_(really_need_rowkey_order),
                        K_(boost_weight));

  storage::ObRow2ExprsProjector *p_row2exprs_projector_;
  ObPushdownOperator *p_pd_expr_op_;
  int64_t tenant_schema_version_;
  int64_t timeout_ts_;
  int64_t tx_lock_timeout_;
  ObSQLMode sql_mode_;
  ObQueryFlag scan_flag_;
  common::ObWrapperAllocatorWithAttr stmt_allocator_;
  common::ObWrapperAllocatorWithAttr scan_allocator_;
  common::ObSEArray<common::ObNewRange, 1> key_ranges_;

  // whether the scalar scan really needs rowkey order, it is equal to the need_rowkey_order_
  // of the scalar ctdef by default and it can be adjusted in the runtime for optimization,
  // for example, the vector index pre-filtering scenario.
  bool really_need_rowkey_order_;
  // Per-execution logical table identity (value-embedded, ns/lt serialized via OB_UNIS).
  ObLogicalTableRtDef lt_rt_;
  // Heavy per-execution state (path_cache, direct_phys_ranges_built_, physical_kv_tablet_id_).
  // Lazy-allocated via ObDASUtils::ensure_lt_scan_rt_allocated in open path.
  ObLogicalTableScanRtDef *lt_scan_rt_;

  OB_INLINE double get_boost_weight() const { return boost_weight_; }
  OB_INLINE void set_boost_weight(const double boost_weight) { boost_weight_ = boost_weight; }
  OB_INLINE int64_t get_estimated_work_row_count() const { return estimated_work_row_count_; }
  OB_INLINE void set_estimated_work_row_count(const int64_t estimated_work_row_count)
  { estimated_work_row_count_ = estimated_work_row_count; }

private:
  int64_t estimated_work_row_count_;
  double boost_weight_;
  union {
    storage::ObRow2ExprsProjector row2exprs_projector_;
  };
  union {
    ObPushdownOperator pd_expr_op_;
  };
};


} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SCALAR_DEFINE_H_
