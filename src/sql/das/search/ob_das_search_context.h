/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


/*
 * Overview
 * - Runtime Context for DAS hybrid search, no need to serialize/deserialize.
 *
 * Key Responsibilities
 * - Share query-level resources: allocator, ls id, transaction/snapshot, attached das task.
 * - Store rowid/score/output expression metadata used by search operators.
 * - Provide utilities: get related tablet id and initialize table-scan parameters, etc.
 * - Manage search operators: creation, cleanup and id assignment.
*/

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_CONTEXT_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_CONTEXT_H_

#include "common/ob_range.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_logical_bm25_scan_host.h"
#include "sql/das/search/ob_das_logical_bm25_stats.h"
#include "sql/das/search/ob_das_logical_bm25_stats_provider.h"
#include "sql/das/search/ob_das_search_define.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "sql/das/search/ob_das_filter_bitmap_provider.h"
#include "storage/retrieval/ob_inv_idx_param_cache.h"
#include "storage/access/ob_async_inv_iterator_pool.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_posting_iter.h"
#include "lib/oblog/ob_log_module.h"
#include "share/logical_table/ob_logical_table_define.h"

namespace oceanbase
{
namespace storage
{
class ObAsyncInvIteratorPool;
}
namespace sql
{

class ObIDASSearchOp;
class RowIDStore;
class ObDASAsyncIndexQueryCtx;
class ObDASAsyncBindingGuard;
struct ObAsyncIndexPartitionSegCtx;
struct ObDASScalarScanCtDef;
struct ObDASScalarScanRtDef;
struct ObDASScanCtDef;

enum ObDASAsyncGroupModeOverride
{
  ASYNC_GROUP_MODE_INHERIT = 0,
  ASYNC_GROUP_MODE_FORCE_LEGACY,
  ASYNC_GROUP_MODE_FORCE_GROUP
};

// Hybrid search sub-tree identity (ns/lt, KV table, tablet). Lighter than ObLogicalTableHelper.
// Populated from ObDASScanOp::lt_ctx_ in init_logical_table_ctx_for_search_. Not PX-serialized.
// See issue/logical_table_das_lt_ctx_layers.md.
struct ObLogicalTableSearchCtx
{
  bool     is_enabled_;
  uint64_t namespace_id_;
  uint64_t logical_table_id_;
  uint64_t physical_data_table_id_;
  share::ObLogicalTableAuxType logical_table_aux_type_;
  common::ObTabletID physical_tablet_id_;

  ObLogicalTableSearchCtx()
    : is_enabled_(false), namespace_id_(0), logical_table_id_(0),
      physical_data_table_id_(common::OB_INVALID_ID),
      logical_table_aux_type_(share::LT_AUX_MAX_TYPE),
      physical_tablet_id_()
  {}
  void reset() { new (this) ObLogicalTableSearchCtx(); }
  TO_STRING_KV(K_(is_enabled), K_(namespace_id), K_(logical_table_id),
               K_(physical_data_table_id), K_(logical_table_aux_type), K_(physical_tablet_id));
};

class ObDASSearchCtx : public ObILogicalBm25ScanHost
{

friend class ObIDASSearchOp;
friend class ObDASSearchDriverIterParam;
friend class ObDASSearchDriverIter;
friend class RowIDStore;
friend class UInt64RowIDStore;
friend class CompactRowIDStore;
friend struct ObDASFusionTaskExecEnv;
friend struct ObDASFusionChildRuntime;
friend class ObDASAsyncBindingGuard;

public:
  explicit ObDASSearchCtx(common::ObIAllocator &allocator, ObDASScanOp &op)
    : allocator_(allocator),
      ls_id_(op.get_ls_id()),
      tx_desc_(op.get_trans_desc()),
      snapshot_(op.get_snapshot()),
      root_das_task_(op),
      eval_ctx_(nullptr),
      rowid_meta_(),
      rowid_exprs_(nullptr),
      output_(nullptr),
      op_count_(0),
      mock_skip_(nullptr),
      bm25_param_cache_(),
      use_dynamic_pruning_(true),
      generating_topk_limit_(-1),
      streaming_ror_costing_topk_hint_(-1),
      rowid_type_(DAS_ROWID_TYPE_UINT64),
      table_row_count_(),
      row_count_logical_table_ctx_(),
      is_vec_index_search_(false),
      async_query_ctx_(nullptr),
      docid_range_lo_(),
      docid_range_hi_(),
      has_docid_range_(false),
      logical_table_ctx_(),
      nested_scope_id_(DAS_SCOPE_MAX),
      op_creating_scope_(DAS_SCOPE_BASE),
      op_creating_depth_(0),
      current_async_partition_(nullptr),
      async_group_mode_override_(ASYNC_GROUP_MODE_INHERIT),
      async_index_table_id_(common::OB_INVALID_ID),
      async_group_slot_(-1),
      async_direct_bind_(false),
      generating_group_filter_domain_(false),
      filter_bitmap_providers_(),
      inv_iter_pool_(nullptr),
      enable_inv_iter_pool_(false),
      posting_table_cache_(),
      fusion_ctdef_(nullptr)
  { }

  ~ObDASSearchCtx() { reset(); }

  TO_STRING_KV(K(ls_id_), K(tx_desc_), K(snapshot_), KPC(eval_ctx_), KPC(rowid_exprs_), K(op_count_));

public:
  int init(
    const sql::ExprFixedArray &rowid_exprs,
    const sql::ExprFixedArray &output,
    bool use_dynamic_pruning);

  /*
   * Get the related tablet id from the attached das task using scan ctdef.
   * Called by search op when open/rescan.
  */
  int get_related_tablet_id(const ObDASScalarScanCtDef *scalar_ctdef, common::ObTabletID &tablet_id);

  // Register one nested mode before creating operators. The scope fixes its
  // RowID layout: ASYNC uses doc_id; SYNC retains CompactRow (doc_id, NK).
  int register_nested_scope(const sql::ExprFixedArray *nested_rowid_exprs,
                            ObDASScopeId scope_id);
  ObDASScopeId get_nested_scope_id() const { return nested_scope_id_; }

  // Fills scan_param metadata from ctdef/rtdef. Does not copy key_ranges_;
  // callers that table_scan the full domain must assign rtdef->key_ranges_
  // themselves, or bind a subset (e.g. one async ROR leaf range).
  int init_scan_param(common::ObTabletID &tablet_id,
                      const ObDASScalarScanCtDef *ctdef,
                      ObDASScalarScanRtDef *rtdef,
                      ObTableScanParam &scan_param,
                      bool use_async_read_snapshot = true);
  int apply_async_index_read_snapshot(
      storage::ObTableScanParam &scan_param) const;

  // ObILogicalBm25ScanHost overrides — base-type entry points used by the
  // Provider. Hybrid path only ever passes scalar ctdefs/rtdefs, so we cast
  // back to scalar internally and delegate to the existing overloads.
  virtual int get_related_tablet_id(const ObDASBaseCtDef *ctdef,
                                    common::ObTabletID &tablet_id) override;
  virtual int init_scan_param(common::ObTabletID &tablet_id,
                              const ObDASBaseCtDef *ctdef,
                              ObDASBaseRtDef *rtdef,
                              ObTableScanParam &scan_param) override;

  OB_INLINE bool is_valid() const
  { return nullptr != eval_ctx_ && nullptr != rowid_exprs_ && nullptr != output_ && nullptr != mock_skip_ && table_row_count_.is_valid(); }
  OB_INLINE int64_t max_batch_size() const { return eval_ctx_->max_batch_size_; }
  OB_INLINE storage::ObInvIdxParamCache &get_bm25_param_cache() { return bm25_param_cache_; }
  // A null Entry means that the caller must use its original allocation path.
  int acquire_posting_entry(blocksstable::ObInvPostingIterEntry *&entry);
  int prepare_posting_scan(const bool use_pool, common::ObIAllocator &scan_allocator,
      blocksstable::ObInvPostingScanParam &param, blocksstable::ObInvPostingIterEntry *&entry);
  void close_posting_scan(blocksstable::ObInvPostingScanParam &param,
      common::ObIAllocator &scan_allocator, blocksstable::ObInvPostingIterEntry *&entry,
      blocksstable::ObInvSSTablePostingIter *&iter);
  blocksstable::ObInvPostingTableCache &get_posting_table_cache() { return posting_table_cache_; }
  OB_INLINE RowMeta &get_rowid_meta() { return rowid_meta_; }
  OB_INLINE const transaction::ObTxReadSnapshot *get_snapshot() const { return snapshot_; }
  OB_INLINE const common::ObIArray<ObExpr *> &get_rowid_exprs() const { return *rowid_exprs_; }
  OB_INLINE const ObDASRowIDScope &get_scope(ObDASScopeId id) const { return scopes_[id]; }
  OB_INLINE common::ObIAllocator &get_allocator() { return allocator_; }
  OB_INLINE const common::ObTabletID &get_root_tablet_id() const
  { return root_das_task_.get_scan_param().tablet_id_; }
  virtual ObEvalCtx *get_eval_ctx() override { return eval_ctx_; }
  bool is_serial_fusion_plan() const;
  /** @brief Bind the Fusion definition that owns this task's pagination metadata. */
  void set_fusion_ctdef(const ObDASFusionCtDef *ctdef) { fusion_ctdef_ = ctdef; }
  /** @brief Whether TableScan must apply search_after after the main-table lookup. */
  bool has_search_after() const { return fusion_ctdef_ != nullptr && fusion_ctdef_->has_search_after_; }
  /** @brief Whether DAS must supply __shard_doc for SQL sorting or __sort output. */
  bool has_shard_doc() const { return fusion_ctdef_ != nullptr && fusion_ctdef_->shard_doc_expr_ != nullptr; }
  /**
   * @brief Projects __shard_doc into one row of the sort expression vector.
   * @param[in] increment Document increment paired with the current partition's main tablet ID.
   * @param[in] index Output row position; position zero initializes the vector for this batch.
   * @param[in] count Number of rows in the output batch.
   * @return OB_SUCCESS, including when no __shard_doc key is required; an error for invalid state or allocation failure.
   */
  int project_shard_doc(uint64_t increment, int64_t index, int64_t count);
  /** @brief Return the sync task's main tablet or the bound async partition's main tablet. */
  common::ObTabletID get_main_tablet_id() const;
  OB_INLINE const share::ObLSID &get_ls_id() const { return ls_id_; }
  OB_INLINE void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  void reset();
  void set_docid_range(const common::ObObj &lo, const common::ObObj &hi)
  {
    docid_range_lo_ = lo;
    docid_range_hi_ = hi;
    has_docid_range_ = true;
  }
  OB_INLINE bool has_docid_range() const { return has_docid_range_; }
  OB_INLINE const common::ObObj &get_docid_range_lo() const { return docid_range_lo_; }
  OB_INLINE const common::ObObj &get_docid_range_hi() const { return docid_range_hi_; }

  int init_logical_table_ctx(const ObDASBaseCtDef *mock_ctdef);
  virtual const ObLogicalTableSearchCtx &get_logical_table_ctx() const override { return logical_table_ctx_; }
  ObLogicalTableSearchCtx &get_logical_table_ctx() { return logical_table_ctx_; }
  OB_INLINE bool is_logical_table() const { return logical_table_ctx_.is_enabled_; }

  /// Look up an existing query-scope provider matching `key`. Returns nullptr if absent.
  ObLogicalFilteredBM25StatsProvider *find_logical_bm25_stats_provider(
      const ObLogicalFilteredBM25StatsKey &key);
  /// Allocate, init and register a new provider in the query-scope cache.
  /// Returns the registered instance via `provider`. Reuses the search ctx allocator.
  int get_or_create_logical_bm25_stats_provider(
      const ObLogicalFilteredBM25StatsKey &key,
      const ObDASScalarScanCtDef *primary_stats_ctdef,
      ObDASScalarScanRtDef *primary_stats_rtdef,
      const ObDASScalarScanCtDef *inv_scan_ctdef,
      ObDASScalarScanRtDef *inv_scan_rtdef,
      const sql::ExprFixedArray *filter_exprs,
      ObLogicalFilteredBM25StatsProvider *&provider);

  ObDASFilterBitmapProvider *find_filter_bitmap_provider(
      const ObIDASSearchRtDef *owner_rtdef,
      const ObDASFilterBitmapProviderScope scope,
      const int64_t group_slot) const;
  int create_filter_bitmap_provider(
      const ObIDASSearchRtDef *owner_rtdef,
      const ObDASFilterBitmapProviderScope scope,
      const int64_t group_slot,
      ObIDASSearchOp *source_op,
      ObDASFilterBitmapProvider *&provider);
  int reset_filter_bitmap_providers_for_rescan();
  OB_INLINE int64_t get_filter_bitmap_provider_count() const
  { return filter_bitmap_providers_.count(); }
  ObDASFilterBitmapProvider *get_filter_bitmap_provider(const int64_t idx) const
  {
    return idx >= 0 && idx < filter_bitmap_providers_.count()
        ? filter_bitmap_providers_.at(idx) : nullptr;
  }

  OB_INLINE ObDASRowIDType get_rowid_type() const { return rowid_type_; }
  OB_INLINE void set_rowid_type(ObDASRowIDType type) { rowid_type_ = type; }
  OB_INLINE bool is_uint64_rowid() const { return DAS_ROWID_TYPE_UINT64 == rowid_type_; }
  OB_INLINE const ObDASSearchCost &get_row_count() const { return table_row_count_; }
  OB_INLINE int64_t get_generating_topk_limit() const { return generating_topk_limit_; }
  OB_INLINE void set_generating_topk_limit(int64_t limit) { generating_topk_limit_ = limit; }
  OB_INLINE int64_t get_streaming_ror_costing_topk_hint() const { return streaming_ror_costing_topk_hint_; }
  OB_INLINE void set_streaming_ror_costing_topk_hint(int64_t hint) { streaming_ror_costing_topk_hint_ = hint; }
  OB_INLINE bool use_dynamic_pruning() const { return use_dynamic_pruning_; }
  OB_INLINE void set_is_vec_index_search(bool is_vec_index_search) { is_vec_index_search_ = is_vec_index_search; }
  OB_INLINE bool is_vec_index_search() { return is_vec_index_search_; }
  OB_INLINE void set_async_query_ctx(ObDASAsyncIndexQueryCtx *ctx) { async_query_ctx_ = ctx; }
  OB_INLINE ObDASAsyncIndexQueryCtx *get_async_query_ctx() const { return async_query_ctx_; }
  OB_INLINE bool is_async_index_query() const { return nullptr != async_query_ctx_; }
  bool use_async_group_alignment() const;
  OB_INLINE ObDASAsyncGroupModeOverride get_async_group_mode_override() const
  { return async_group_mode_override_; }
  OB_INLINE void set_async_group_mode_override(const ObDASAsyncGroupModeOverride mode)
  { async_group_mode_override_ = mode; }
  OB_INLINE void set_current_async_partition(const ObAsyncIndexPartitionSegCtx *partition)
  { current_async_partition_ = partition; }
  OB_INLINE const ObAsyncIndexPartitionSegCtx *get_current_async_partition() const
  { return current_async_partition_; }
  int resolve_direct_async_index_segment(
      const uint64_t index_table_id,
      const common::ObTabletID &index_tablet_id,
      const int64_t group_slot,
      common::ObNewVersionRange &version_range,
      bool &is_empty) const;
  OB_INLINE bool has_nested_scope() const
  { return is_nested_scope(nested_scope_id_); }
  OB_INLINE void set_async_index_table_id(const uint64_t index_table_id)
  { async_index_table_id_ = index_table_id; }
  OB_INLINE uint64_t get_async_index_table_id() const { return async_index_table_id_; }
  OB_INLINE void set_async_group_slot(const int64_t group_slot)
  { async_group_slot_ = group_slot; }
  OB_INLINE int64_t get_async_group_slot() const { return async_group_slot_; }
  OB_INLINE void set_async_direct_bind(const bool direct_bind)
  { async_direct_bind_ = direct_bind; }
  OB_INLINE bool get_async_direct_bind() const { return async_direct_bind_; }
  OB_INLINE bool is_generating_group_filter_domain() const
  { return generating_group_filter_domain_; }
  OB_INLINE void set_generating_group_filter_domain(const bool is_generating)
  { generating_group_filter_domain_ = is_generating; }
  OB_INLINE void set_async_binding(
      const uint64_t index_table_id,
      const int64_t group_slot,
      const bool direct_bind)
  {
    async_index_table_id_ = index_table_id;
    async_group_slot_ = group_slot;
    async_direct_bind_ = direct_bind;
  }
  int refresh_table_row_count();
  // Re-estimate table_row_count_ only when the root das task has switched to a
  // different tablet, so per-partition BM25 estimation sees the current partition.
  int refresh_table_row_count_if_needed();
  int lower_bound_in_frame(ObDASScopeId scope, const ObDASRowID &target,
                           const ObIArray<ObExpr *> *rowid_exprs,
                           const int64_t count, int64_t &idx);
  int estimate_row_count(const ObDASScalarScanCtDef *ctdef, const ObDASScalarScanRtDef *rtdef, int64_t &row_count);

  /*
   * RowID utility methods - type-agnostic operations on ObDASRowID.
   * These methods internally dispatch to the correct implementation based on rowid_type_.
   * The scope-aware overloads (taking ObDASScopeId) look up scopes_[scope] for dispatch.
   * The scope-less overloads dispatch via the top-level rowid_type_ (equivalent to BASE scope).
   */
  int compare_rowid(const ObDASRowID &rowid1, const ObDASRowID &rowid2, int &cmp) const
  { return all_rowid_funcs_[rowid_type_].compare_(rowid1, rowid2, rowid_meta_, *rowid_exprs_, cmp); }
  int deep_copy_rowid(const ObDASRowID &src, ObDASRowID &dst, common::ObIAllocator &alloc) const
  { return all_rowid_funcs_[rowid_type_].deep_copy_(src, dst, alloc); }
  int create_rowid_store(int64_t capacity, RowIDStore *&store)
  { return all_rowid_funcs_[rowid_type_].create_store_(*this, capacity, store, allocator_, DAS_SCOPE_BASE); }
  int rowid_to_expr(const ObDASRowID &rowid, int64_t batch_idx)
  { return all_rowid_funcs_[rowid_type_].to_expr_(rowid, *rowid_exprs_, rowid_meta_, *eval_ctx_, batch_idx); }
  int write_datum_to_rowid(const ObDatum &datum, ObDASRowID &rowid, common::ObIAllocator &alloc) const
  { return all_rowid_funcs_[rowid_type_].write_datum_(datum, rowid_meta_, *rowid_exprs_, rowid, alloc); }
  int get_datum_from_rowid(const ObDASRowID &rowid, ObDatum &datum, int64_t col_idx = 0) const
  { return all_rowid_funcs_[rowid_type_].get_datum_(rowid, rowid_meta_, datum, col_idx); }

  // Scope-aware overloads: dispatch through scopes_[scope]
  int compare_rowid(ObDASScopeId scope, const ObDASRowID &r1, const ObDASRowID &r2, int &cmp) const
  { return all_rowid_funcs_[scopes_[scope].rowid_type_].compare_(r1, r2, scopes_[scope].rowid_meta_, *scopes_[scope].rowid_exprs_, cmp); }
  int deep_copy_rowid(ObDASScopeId scope, const ObDASRowID &src, ObDASRowID &dst, common::ObIAllocator &alloc) const
  { return all_rowid_funcs_[scopes_[scope].rowid_type_].deep_copy_(src, dst, alloc); }
  int create_rowid_store(ObDASScopeId scope, int64_t capacity, RowIDStore *&store)
  { return all_rowid_funcs_[scopes_[scope].rowid_type_].create_store_(*this, capacity, store, allocator_, scope); }
  int rowid_to_expr(ObDASScopeId scope, const ObDASRowID &rowid, int64_t batch_idx)
  { return all_rowid_funcs_[scopes_[scope].rowid_type_].to_expr_(rowid, *scopes_[scope].rowid_exprs_, scopes_[scope].rowid_meta_, *eval_ctx_, batch_idx); }
  int write_datum_to_rowid(ObDASScopeId scope, const ObDatum &datum, ObDASRowID &rowid, common::ObIAllocator &alloc) const
  { return all_rowid_funcs_[scopes_[scope].rowid_type_].write_datum_(datum, scopes_[scope].rowid_meta_, *scopes_[scope].rowid_exprs_, rowid, alloc); }
  // Writes N column datums into a compact rowid for the given scope.
  // For BASE scope (single uint64 column): equivalent to write_datum_to_rowid(BASE, datums[0], ...).
  // For NESTED scope (multi-column compact row): packs each datum[i] into column i.
  int write_datums_to_rowid(ObDASScopeId scope, const common::ObIArray<ObDatum> &datums,
                            ObDASRowID &rowid, common::ObIAllocator &alloc) const;
  int get_datum_from_rowid(ObDASScopeId scope, const ObDASRowID &rowid, ObDatum &datum, int64_t col_idx = 0) const
  { return all_rowid_funcs_[scopes_[scope].rowid_type_].get_datum_(rowid, scopes_[scope].rowid_meta_, datum, col_idx); }

  OB_INLINE ObDASScopeId get_op_creating_scope() const { return op_creating_scope_; }
  OB_INLINE int32_t get_op_creating_depth() const { return op_creating_depth_; }
  OB_INLINE void enter_scope_for_op_creating(ObDASScopeId new_scope, int32_t depth, ObDASScopeId &backup_scope)
  {
    backup_scope = op_creating_scope_;
    op_creating_scope_ = new_scope;
    op_creating_depth_ = depth;
  }
  OB_INLINE void exit_scope_after_op_created(ObDASScopeId backup_scope, int32_t backup_depth) {
    op_creating_scope_ = backup_scope;
    op_creating_depth_ = backup_depth;
  }

public:
  template<class Op, class OpParam>
  int create_op(const OpParam &op_param, Op *&op)
  {
    int ret = OB_SUCCESS;
    Op *result = nullptr;
    op = nullptr;
    if (OB_ISNULL(result = OB_NEWx(Op, &allocator_, *this))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate search op", K(ret));
    } else if (OB_FAIL(result->init(op_param))) {
      LOG_WARN("failed to init search op", K(ret));
      result->~Op();
      allocator_.free(result);
      result = nullptr;
    } else if (OB_FAIL(op_store_.push_back(result))) {
      LOG_WARN("failed to push back op", K(ret));
      const int close_ret = result->close();
      if (OB_SUCCESS != close_ret) {
        LOG_WARN("failed to close unregistered search op", K(close_ret));
      }
      result->~Op();
      allocator_.free(result);
      result = nullptr;
    } else {
      op = result;
      op->set_op_id(op_count_++);
    }
    return ret;
  }

private:
  struct RowIDFuncs
  {
    int (*compare_)(const ObDASRowID &rowid1, const ObDASRowID &rowid2,
                    const RowMeta &rowid_meta, const ExprFixedArray &rowid_exprs,
                    int &cmp);
    int (*deep_copy_)(const ObDASRowID &src, ObDASRowID &dst,
                      common::ObIAllocator &alloc);
    int (*create_store_)(ObDASSearchCtx &ctx, int64_t capacity, RowIDStore *&store,
                         common::ObIAllocator &alloc, ObDASScopeId scope);
    int (*to_expr_)(const ObDASRowID &rowid, const ExprFixedArray &rowid_exprs,
                    const RowMeta &rowid_meta, ObEvalCtx &eval_ctx, int64_t batch_idx);
    int (*write_datum_)(const ObDatum &datum, const RowMeta &meta,
                        const ExprFixedArray &rowid_exprs, ObDASRowID &rowid,
                        common::ObIAllocator &alloc);
    int (*get_datum_)(const ObDASRowID &rowid, const RowMeta &meta, ObDatum &datum,
                      int64_t col_idx);
  };

private:
  common::ObIAllocator &allocator_;
  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  ObDASScanOp &root_das_task_;
  ObEvalCtx *eval_ctx_;
  RowMeta rowid_meta_;
  const sql::ExprFixedArray *rowid_exprs_;
  const sql::ExprFixedArray *output_;
  int64_t op_count_;
  ObSEArray<ObIDASSearchOp *, 16> op_store_;
  sql::ObBitVector *mock_skip_;
  storage::ObInvIdxParamCache bm25_param_cache_;
  bool use_dynamic_pruning_;
  // Scoped to descendant operator generation; never a bound on returned candidates.
  int64_t generating_topk_limit_;
  // Build-time hint for streaming ROR costing only; -1 disables the estimate.
  // Never changes the actual TopK limit or limits candidates/output rows.
  int64_t streaming_ror_costing_topk_hint_;
  ObDASRowIDType rowid_type_;
  ObDASSearchCost table_row_count_;
  // tablet that table_row_count_ was estimated on; valid iff table_row_count_ is valid
  common::ObTabletID row_count_tablet_id_;
  // Logical-table identity used by table_row_count_. The initial estimate is made
  // before logical_table_ctx_ is populated, so this must participate in cache validity.
  ObLogicalTableSearchCtx row_count_logical_table_ctx_;
  bool is_vec_index_search_;
  ObDASAsyncIndexQueryCtx *async_query_ctx_; // not owned; set by ObDASScanOp during iter tree build
  common::ObObj docid_range_lo_;
  common::ObObj docid_range_hi_;
  bool has_docid_range_;
  ObLogicalTableSearchCtx logical_table_ctx_;
  // Query-scope cache for filtered BM25 stats providers. Linear scan: typical
  // hybrid_search has 1 IR ctdef and a small number of distinct filter sets.
  ObSEArray<ObLogicalFilteredBM25StatsProvider *, 4> logical_bm25_stats_providers_;
  ObDASRowIDScope scopes_[DAS_SCOPE_MAX];
  ObDASScopeId nested_scope_id_; // MAX until one nested mode is registered.
  // only used for create op with specific scope
  // not used after das op tree is created
  ObDASScopeId op_creating_scope_;
  int32_t op_creating_depth_;
  const ObAsyncIndexPartitionSegCtx *current_async_partition_; // worker-local cursor into shared catalog
  ObDASAsyncGroupModeOverride async_group_mode_override_; // Temporary subtree-local override during op tree build
  uint64_t async_index_table_id_; // Used only while building the largest async-index subtree
  int64_t async_group_slot_; // Used only while building the fixed async Group template
  bool async_direct_bind_; // Used only for a single-Segment subtree without SegmentMerge
  // Construction-only context propagated while a Boolean FILTER subtree is generated.
  // It distinguishes a validated pure synchronous Group filter from a mixed query root.
  bool generating_group_filter_domain_;
  // Explicit Group-scope provider registry. Entries from different Groups are
  // isolated by group_slot. Provider ordinals remain deterministic across
  // Fusion parallel RtDef clones and are used for shared-bitmap injection.
  ObSEArray<ObDASFilterBitmapProvider *, 4> filter_bitmap_providers_;
  // Holds one pool reference until reset(); there is no query-local Entry cache.
  storage::ObAsyncInvIteratorPool *inv_iter_pool_;
  bool enable_inv_iter_pool_;
  blocksstable::ObInvPostingTableCache posting_table_cache_;
  const ObDASFusionCtDef *fusion_ctdef_; // Borrowed pagination metadata; owned by the DAS task's plan.

private:
  struct EstimateScanInput
  {
    EstimateScanInput(
        const ObDASScalarScanCtDef *related_tablet_ctdef,
        const common::ObIArray<common::ObNewRange> &key_ranges,
        const uint64_t ref_table_id,
        const int64_t schema_version,
        const ObQueryFlag &scan_flag,
        const bool is_async_scalar_range,
        const bool is_logical_table,
        const uint64_t logical_index_seq,
        const share::ObLogicalTableAuxType logical_table_aux_type,
        common::ObIAllocator &range_allocator)
      : related_tablet_ctdef_(related_tablet_ctdef),
        key_ranges_(key_ranges),
        ref_table_id_(ref_table_id),
        schema_version_(schema_version),
        scan_flag_(scan_flag),
        is_async_scalar_range_(is_async_scalar_range),
        is_logical_table_(is_logical_table),
        logical_index_seq_(logical_index_seq),
        logical_table_aux_type_(logical_table_aux_type),
        range_allocator_(range_allocator)
    {}

    const ObDASScalarScanCtDef *related_tablet_ctdef_;
    const common::ObIArray<common::ObNewRange> &key_ranges_;
    uint64_t ref_table_id_;
    int64_t schema_version_;
    const ObQueryFlag &scan_flag_;
    bool is_async_scalar_range_;
    bool is_logical_table_;
    uint64_t logical_index_seq_;
    share::ObLogicalTableAuxType logical_table_aux_type_;
    common::ObIAllocator &range_allocator_;
  };

  int build_estimate_scan_param(
      const EstimateScanInput &input,
      const bool whole_ltable_range,
      ObTableScanParam &est_param,
      common::ObIArray<common::ObNewRange> &estimate_ranges);

  bool is_row_count_logical_table_ctx_current() const
  {
    const ObLogicalTableSearchCtx &cached_ctx = row_count_logical_table_ctx_;
    const ObLogicalTableSearchCtx &current_ctx = logical_table_ctx_;
    return cached_ctx.is_enabled_ == current_ctx.is_enabled_
        && (!current_ctx.is_enabled_
            || (cached_ctx.namespace_id_ == current_ctx.namespace_id_
                && cached_ctx.logical_table_id_ == current_ctx.logical_table_id_
                && cached_ctx.physical_data_table_id_ == current_ctx.physical_data_table_id_
                && cached_ctx.logical_table_aux_type_ == current_ctx.logical_table_aux_type_
                && cached_ctx.physical_tablet_id_ == current_ctx.physical_tablet_id_));
  }

  static int compare_uint64_rowid(const ObDASRowID &rowid1, const ObDASRowID &rowid2,
                                  const RowMeta &rowid_meta, const ExprFixedArray &rowid_exprs,
                                  int &cmp);
  static int compare_compact_rowid(const ObDASRowID &rowid1, const ObDASRowID &rowid2,
                                   const RowMeta &rowid_meta, const ExprFixedArray &rowid_exprs,
                                   int &cmp);

  static int deep_copy_uint64_rowid(const ObDASRowID &src, ObDASRowID &dst,
                                    common::ObIAllocator &alloc);
  static int deep_copy_compact_rowid(const ObDASRowID &src, ObDASRowID &dst,
                                     common::ObIAllocator &alloc);

  template<typename StoreType>
  static int create_rowid_store(ObDASSearchCtx &ctx, int64_t capacity, RowIDStore *&store,
                                common::ObIAllocator &alloc, ObDASScopeId scope);

  static int uint64_rowid_to_expr(const ObDASRowID &rowid, const ExprFixedArray &rowid_exprs,
                                  const RowMeta &rowid_meta, ObEvalCtx &eval_ctx, int64_t batch_idx);
  static int compact_rowid_to_expr(const ObDASRowID &rowid, const ExprFixedArray &rowid_exprs,
                                   const RowMeta &rowid_meta, ObEvalCtx &eval_ctx, int64_t batch_idx);

  static int write_datum_to_uint64_rowid(const ObDatum &datum, const RowMeta &meta,
                                         const ExprFixedArray &rowid_exprs, ObDASRowID &rowid,
                                         common::ObIAllocator &alloc);
  static int write_datum_to_compact_rowid(const ObDatum &datum, const RowMeta &meta,
                                          const ExprFixedArray &rowid_exprs, ObDASRowID &rowid,
                                          common::ObIAllocator &alloc);
  // Shared helper: pack N datums into a newly-allocated ObCompactRow, set rowid.
  static int build_compact_rowid(const common::ObIArray<ObDatum> &datums,
                                 const ExprFixedArray &rowid_exprs,
                                 const RowMeta &meta,
                                 ObDASRowID &rowid,
                                 common::ObIAllocator &alloc);

  static int get_datum_from_uint64_rowid(const ObDASRowID &rowid, const RowMeta &meta, ObDatum &datum,
                                         int64_t col_idx);
  static int get_datum_from_compact_rowid(const ObDASRowID &rowid, const RowMeta &meta, ObDatum &datum,
                                          int64_t col_idx);

  static const RowIDFuncs all_rowid_funcs_[DAS_ROWID_TYPE_MAX];
  DISALLOW_COPY_AND_ASSIGN(ObDASSearchCtx);
};

// Temporary binding guard used during tree construction. Restores bindings on scope exit
// to prevent sibling Groups or indexes from overwriting each other.
class ObDASAsyncBindingGuard
{
public:
  explicit ObDASAsyncBindingGuard(ObDASSearchCtx &search_ctx)
    : search_ctx_(search_ctx),
      index_table_id_(search_ctx.get_async_index_table_id()),
      group_slot_(search_ctx.get_async_group_slot()),
      direct_bind_(search_ctx.get_async_direct_bind())
  {}
  ~ObDASAsyncBindingGuard()
  {
    search_ctx_.set_async_binding(index_table_id_, group_slot_, direct_bind_);
  }
  void set(
      const uint64_t index_table_id,
      const int64_t group_slot,
      const bool direct_bind)
  {
    search_ctx_.set_async_binding(index_table_id, group_slot, direct_bind);
  }

private:
  ObDASSearchCtx &search_ctx_;
  uint64_t index_table_id_;
  int64_t group_slot_;
  bool direct_bind_;
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncBindingGuard);
};

// Temporarily overrides Group/Legacy selection while one independent async query subtree is built.
// The generated operator topology retains the decision after this guard restores the parent mode.
class ObDASAsyncGroupModeGuard
{
public:
  ObDASAsyncGroupModeGuard(ObDASSearchCtx &search_ctx, const bool use_group)
    : search_ctx_(search_ctx),
      old_mode_(search_ctx.get_async_group_mode_override())
  {
    search_ctx_.set_async_group_mode_override(
        use_group ? ASYNC_GROUP_MODE_FORCE_GROUP : ASYNC_GROUP_MODE_FORCE_LEGACY);
  }
  ~ObDASAsyncGroupModeGuard()
  { search_ctx_.set_async_group_mode_override(old_mode_); }

private:
  ObDASSearchCtx &search_ctx_;
  ObDASAsyncGroupModeOverride old_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncGroupModeGuard);
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_CONTEXT_H_
