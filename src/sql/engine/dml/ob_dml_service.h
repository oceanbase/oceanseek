/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef DEV_SRC_SQL_ENGINE_DML_OB_DML_SERVICE_H_
#define DEV_SRC_SQL_ENGINE_DML_OB_DML_SERVICE_H_
#include "sql/engine/dml/ob_dml_ctx_define.h"
#include "sql/das/ob_das_context.h"
#include "sql/das/ob_das_utils.h"
#include "ob_table_modify_op.h"
#include "storage/tx_storage/ob_access_service.h"
#include "share/ob_debug_sync.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace share
{
struct ObVectorIndexAcquireCtx;
}
namespace sql
{
class ObTableModifyOp;
class ObErrLogService;
class ObDMLService
{
public:
  static bool check_cascaded_reference(const ObExpr *expr, const ObExprPtrIArray &row);

  static int check_row_null(const ObExprPtrIArray &row,
                            ObEvalCtx &eval_ctx,
                            int64_t row_num,
                            const ColContentIArray &column_infos,
                            bool is_ignore,
                            bool is_single_value,
                            ObTableModifyOp &dml_op);
  static int check_column_type(const ExprFixedArray &dml_row,
                               int64_t row_num,
                               const common::ObIArray<ColumnContent> &column_infos,
                               ObTableModifyOp &dml_op);
  static int check_rowkey_is_null(const ObExprPtrIArray &row,
                                  int64_t rowkey_cnt,
                                  ObEvalCtx &eval_ctx,
                                  bool &is_null);
  static int check_rowkey_whether_distinct(const ObExprPtrIArray &row,
                                           DistinctType distinct_algo,
                                           ObEvalCtx &eval_ctx,
                                           ObExecContext &root_ctx,
                                           ObRowkey &table_rowkey,
                                           SeRowkeyDistCtx *rowkey_dist_ctx,
                                           bool &is_dist);
  
  static int create_rowkey_check_hashset(int64_t estimate_row,
                                           ObExecContext *root_ctx,
                                           SeRowkeyDistCtx *&rowkey_dist_ctx);
  
  static int check_table_cycle(const DASTableIdList* parent_table_set, 
                               const uint64_t table_id, bool &exist);
  static int delete_table_id_from_parent_table_set(ObDMLRtCtx &dml_rtctx, const ObDMLBaseCtDef &dml_ctdef);
  static int check_lob_column_changed(ObEvalCtx &eval_ctx, 
              const ObExpr& old_expr, ObDatum& old_datum,
              const ObExpr& new_expr, ObDatum& new_datum,
              int64_t& result);
  static int check_row_whether_changed(const ObUpdCtDef &upd_ctdef, ObUpdRtDef &upd_rtdef, ObEvalCtx &eval_ctx);
  static int filter_row_for_check_cst(const ExprFixedArray &cst_exprs,
                                      ObEvalCtx &eval_ctx,
                                      bool &filtered);

  static int filter_row_for_view_check(const ExprFixedArray &cst_exprs,
                                      ObEvalCtx &eval_ctx,
                                      bool &filtered);
  static int init_heap_table_pk_for_ins(const ObInsCtDef &ins_ctdef,
                                        ObEvalCtx &eval_ctx);
  // skip_before_row_trigger is only for replaying a row whose BEFORE ROW trigger has already run.
  static int process_insert_row(const ObInsCtDef &ins_ctdef,
                                ObInsRtDef &ins_rtdef,
                                ObTableModifyOp &dml_op,
                                bool &is_check_cst_violated_ignored,
                                const bool skip_before_row_trigger = false);
  static int check_column_null(ObEvalCtx &eval_ctx,
                               const ObExprPtrIArray &row,
                               const ColumnContent &column_info,
                               ObDatum &datum,
                               const bool is_ignore,
                               const bool is_single_value,
                               const ObSQLMode sql_mode);
  static int check_geometry_type(ObEvalCtx &eval_ctx,
                                 const ExprFixedArray &dml_row,
                                 const ColumnContent &column_info,
                                 common::ObIAllocator &allocator,
                                 ObDatum &datum);
  static int check_filter_row(const ObInsCtDef &ins_ctdef,
                              ObEvalCtx &eval_ctx,
                              bool &is_skipped);
  static int check_filter_row(const ObUpdCtDef &upd_ctdef,
                              ObEvalCtx &eval_ctx,
                              bool &is_skipped);
  static int check_error_ret_by_row(const ObInsCtDef &ins_ctdef,
                                    ObTableModifyOp &dml_op,
                                    const int errcode);
  static int process_insert_batch(const ObInsCtDef &ins_ctdef,
                                  ObTableModifyOp &dml_op,
                                  const bool use_rich_format);
  static int check_column_type_batch(const ObInsCtDef &ins_ctdef,
                                     ObTableModifyOp &dml_op,
                                     const bool use_rich_format);
  static int check_geometry_column_batch(const ObInsCtDef &ins_ctdef,
                                         ObTableModifyOp &dml_op,
                                         const ColumnContent &column_info,
                                         const bool use_rich_format);
  static int check_column_null_batch(const ObInsCtDef &ins_ctdef,
                                     ObTableModifyOp &dml_op,
                                     const ColumnContent &column_info,
                                     const bool use_rich_format);
  static int check_filter_row_batch(const ObInsCtDef &ins_ctdef,
                                    ObTableModifyOp &dml_op);
  static int process_before_stmt_trigger(const ObDMLBaseCtDef &dml_ctdef,
                                         ObDMLBaseRtDef &dml_rtdef,
                                         ObDMLRtCtx &dml_rtctx,
                                         const ObDmlEventType &dml_event);
  static int process_after_stmt_trigger(const ObDMLBaseCtDef &dml_ctdef,
                                        ObDMLBaseRtDef &dml_rtdef,
                                        ObDMLRtCtx &dml_rtctx,
                                        const ObDmlEventType &dml_event);
  static int process_delete_row(const ObDelCtDef &del_ctdef,
                                ObDelRtDef &del_rtdef,
                                bool &is_skipped,
                                ObTableModifyOp &dml_op);
  static int process_lock_row(const ObLockCtDef &lock_ctdef,
                                ObLockRtDef &lock_rtdef,
                                bool &is_skipped,
                                ObTableModifyOp &dml_op);
  static int process_update_row(const ObUpdCtDef &upd_ctdef,
                                ObUpdRtDef &upd_rtdef,
                                bool &is_skipped,
                                ObTableModifyOp &dml_op);
  static int insert_row(const ObInsCtDef &ins_ctdef,
                        ObInsRtDef &ins_rtdef,
                        const ObDASTabletLoc *tablet_loc,
                        ObDMLRtCtx &dml_rtctx,
                        ObChunkDatumStore::StoredRow *&stored_row);
  static int insert_row(const ObDASInsCtDef &ins_ctdef,
                        ObDASInsRtDef &ins_rtdef,
                        const ObDASTabletLoc *tablet_loc,
                        ObDMLRtCtx &das_rtctx,
                        const ExprFixedArray &new_row,
                        ObChunkDatumStore::StoredRow *&stored_row);
  static int delete_row(const ObDelCtDef &del_ctdef,
                        ObDelRtDef &del_rtdef,
                        const ObDASTabletLoc *tablet_loc,
                        ObDMLRtCtx &dml_rtctx,
                        ObChunkDatumStore::StoredRow *&stored_row);
  static int update_row(const ObDASUpdCtDef &ctdef,
                        ObDASUpdRtDef &rtdef,
                        const ObDASTabletLoc *tablet_loc,
                        ObDMLRtCtx &dml_rtctx,
                        const ExprFixedArray &full_row);
  static int update_row(const ObUpdCtDef &upd_ctdef,
                        ObUpdRtDef &upd_rtdef,
                        const ObDASTabletLoc *old_tablet_loc,
                        const ObDASTabletLoc *new_tablet_loc,
                        ObDMLRtCtx &dml_rtctx,
                        ObChunkDatumStore::StoredRow *&old_row,
                        ObChunkDatumStore::StoredRow *&new_row,
                        ObChunkDatumStore::StoredRow *&full_row);

  static int split_upd_to_del_and_ins(const ObUpdCtDef &upd_ctdef,
                                      ObUpdRtDef &upd_rtdef,
                                      const ObDASTabletLoc *old_tablet_loc,
                                      const ObDASTabletLoc *new_tablet_loc,
                                      ObDMLRtCtx &dml_rtctx,
                                      ObChunkDatumStore::StoredRow *&old_row,
                                      ObChunkDatumStore::StoredRow *&new_row);

  static int delete_row(const ObDASDelCtDef &ctdef,
                        ObDASDelRtDef &rtdef,
                        const ObDASTabletLoc *tablet_loc,
                        ObDMLRtCtx &das_rtctx,
                        const ExprFixedArray &old_row,
                        ObChunkDatumStore::StoredRow *&stored_row);

  static int lock_row(const ObDASLockCtDef &dlock_ctdef,
                      ObDASLockRtDef &dlock_rtdef,
                      const ObDASTabletLoc *tablet_loc,
                      ObDMLRtCtx &das_rtctx,
                      const ExprFixedArray &old_row);

  static int lock_row(const ObLockCtDef &lock_ctdef,
                      ObLockRtDef &lock_rtdef,
                      const ObDASTabletLoc *tablet_loc,
                      ObDMLRtCtx &dml_rtctx);

  static void init_dml_write_flag(const ObDASDMLBaseCtDef &base_ctdef,
                                  ObDASDMLBaseRtDef &base_rtdef,
                                  concurrent_control::ObWriteFlag &write_flag,
                                  bool is_insert_up_gts_opt);

  static int init_dml_param(const ObDASDMLBaseCtDef &base_ctdef,
                            ObDASDMLBaseRtDef &base_rtdef,
                            transaction::ObTxReadSnapshot &snapshot,
                            const int16_t write_branch_id,
                            common::ObIAllocator &das_alloc,
                            storage::ObStoreCtxGuard &store_ctx_gurad,
                            storage::ObDMLBaseParam &dml_param,
                            bool is_insert_up_gts_opt,
                            const ObDASTabletLoc *main_tablet_loc = nullptr,
                            const common::ObTabletID *main_tablet_id = nullptr);
  static int init_das_dml_rtdef(ObDMLRtCtx &dml_rtctx,
                                const ObDASDMLBaseCtDef &das_ctdef,
                                ObDASDMLBaseRtDef &das_rtdef,
                                const ObDASTableLocMeta *loc_meta);
  static int init_related_das_rtdef(ObDMLRtCtx &dml_rtctx,
                                    const DASDMLCtDefArray &das_ctdefs,
                                    DASDMLRtDefArray &das_rtdefs);
  static int init_ins_rtdef(ObDMLRtCtx &dml_rtctx,
                            ObInsRtDef &ins_rtdef,
                            const ObInsCtDef &ins_ctdef,
                            ObIArray<ObExpr*> &clear_exprs,
                            ObIArray<ObForeignKeyChecker*> &fk_checkers);
  static int init_trigger_for_insert(ObDMLRtCtx &dml_rtctx,
                                     const ObInsCtDef &ins_ctdef,
                                     ObInsRtDef &ins_rtdef,
                                     ObIArray<ObExpr*> &clear_exprs);
  static int init_del_rtdef(ObDMLRtCtx &dml_rtctx, ObDelRtDef &del_rtdef, const ObDelCtDef &del_ctdef);
  static int init_trigger_for_delete(ObDMLRtCtx &dml_rtctx, const ObDelCtDef &del_ctdef, ObDelRtDef &del_rtdef);
  static int init_upd_rtdef(ObDMLRtCtx &dml_rtctx,
                            ObUpdRtDef &upd_rtdef,
                            const ObUpdCtDef &upd_ctdef,
                            ObIArray<ObExpr*> &clear_exprs,
                            ObIArray<ObForeignKeyChecker*> &fk_checkers);
  static int init_das_ins_rtdef_for_update(ObDMLRtCtx &dml_rtctx,
                                           const ObUpdCtDef &upd_ctdef,
                                           ObUpdRtDef &upd_rtdef);
  static int init_das_del_rtdef_for_update(ObDMLRtCtx &dml_rtctx,
                                           const ObUpdCtDef &upd_ctdef,
                                           ObUpdRtDef &upd_rtdef);
  static int init_das_lock_rtdef_for_update(ObDMLRtCtx &dml_rtctx,
                                           const ObUpdCtDef &upd_ctdef,
                                           ObUpdRtDef &upd_rtdef);
  static int init_trigger_for_update(ObDMLRtCtx &dml_rtctx,
                                      const ObUpdCtDef &upd_ctdef,
                                      ObUpdRtDef &upd_rtdef,
                                      ObTableModifyOp &dml_op,
                                      ObIArray<ObExpr*> &clear_exprs);
  static int init_lock_rtdef(ObDMLRtCtx &dml_rtctx,
                             const ObLockCtDef &lock_ctdef,
                             ObLockRtDef &lock_rtdef,
                             int64_t wait_ts);
  static int convert_exprs_to_stored_row(common::ObIAllocator &allocator,
                                         ObEvalCtx &eval_ctx,
                                         const ObExprPtrIArray &exprs,
                                         ObChunkDatumStore::StoredRow *&new_row);
  static int catch_violate_error(int err_ret,
                                 transaction::ObTxSEQ savepoint_no,
                                 ObDMLRtCtx &dml_rtctx,
                                 ObErrLogRtDef &err_log_rt_def,
                                 ObErrLogCtDef &error_logging_ctdef,
                                 ObErrLogService &err_log_service,
                                 ObDASOpType type);

  static int write_one_row_post_proc(ObDMLRtCtx &dml_rtctx);

  static int set_update_hidden_pk(ObEvalCtx &eval_ctx,
                                  const ObUpdCtDef &upd_ctdef,
                                  const common::ObTabletID &tablet_id);

  static int copy_heap_table_hidden_pk(ObEvalCtx &eval_ctx,
                                       const ObUpdCtDef &upd_ctdef);

  static int get_heap_table_hidden_pk(uint64_t tenant_id,
                                      const common::ObTabletID &tablet_id,
                                      uint64_t &pk);

  static int set_heap_table_hidden_pk(const ObInsCtDef &ins_ctdef,
                                      const common::ObTabletID &tablet_id,
                                      ObEvalCtx &eval_ctx);
  static int create_anonymous_savepoint(transaction::ObTxDesc &tx_desc, transaction::ObTxSEQ &savepoint, int16_t branch_id = 0);
  static int rollback_local_savepoint(transaction::ObTxDesc &tx_desc,
                                      const transaction::ObTxSEQ savepoint,
                                      int64_t expire_ts);
  static int check_local_index_affected_rows(int64_t table_affected_rows,
                                             int64_t index_affected_rows,
                                             const ObDASDMLBaseCtDef &ctdef,
                                             ObDASDMLBaseRtDef &rtdef,
                                             const ObDASDMLBaseCtDef &related_ctdef,
                                             ObDASDMLBaseRtDef &related_rtdef);
  static int check_dml_tablet_validity(ObDMLRtCtx &dml_rtctx,
                                       const ObDASTabletLoc &tablet_loc,
                                       const ExprFixedArray &row,
                                       const ObDMLBaseCtDef &dml_ctdef,
                                       ObDMLBaseRtDef &dml_rtdef);
  static bool is_nested_dup_table(const uint64_t table_id,DASDelCtxList& del_ctx_list);
  static int get_nested_dup_table_ctx(const uint64_t table_id,
                                      DASDelCtxList& del_ctx_list,
                                      SeRowkeyDistCtx *&rowkey_dist_ctx);
  static int handle_after_processing_single_row(ObDMLModifyRowsList *dml_modify_rows,
                                                const bool restore_rows);
  static int handle_after_processing_single_row(ObDMLModifyRowNode &modify_row,
                                                const bool restore_rows = false);
  static int handle_after_processing_multi_row(ObDMLModifyRowsList *dml_modify_rows, ObTableModifyOp *op);
  // static int handle_after_processing_batch(const ObTableModifyOp *op,
  //                                           ObDMLModifyRowsList *dml_modify_rows);
  // restore_rows is required when single-row processing has overwritten the exprs of earlier rows,
  // e.g. conflict resolution of REPLACE deletes several old rows for one new row.
  static int handle_after_row_processing(ObTableModifyOp *op,
                                         ObDMLModifyRowsList *dml_modify_rows,
                                         const bool restore_rows = false);
  static int check_das_tasks_unstarted(ObDASRef &das_ref);
  static int init_ob_rowkey( ObIAllocator &allocator, const int64_t rowkey_cnt, ObRowkey &table_rowkey);
  static int add_trans_info_datum(ObExpr *trans_info_expr,
                                  ObEvalCtx &eval_ctx,
                                  ObChunkDatumStore::StoredRow *stored_row);
  static int init_fk_checker_array(ObDMLRtCtx &dml_rtctx,
                                   const ObDMLBaseCtDef &dml_ctdef,
                                   FkCheckerArray &fk_checker_array);
  static int log_user_error_inner(int ret,
                                  int64_t row_num,
                                  common::ObString &column_name,
                                  ObExecContext &ctx,
                                  const ObObjType column_type);
  static int get_root_exec_ctx_for_fk_cascading(ObExecContext *ctx, ObExecContext* &needed_ctx);

private:
  static int check_agg_task_state(ObDMLRtCtx &dml_rtctx, ObIDASTaskOp *das_op, int64_t row_size, bool &reach_mem_limit);
  static int parallel_submit_das_task(ObDMLRtCtx &dml_rtctx, ObDasAggregatedTask *agg_task);
  template <int N>
  static int write_row_to_das_op(const ObDASDMLBaseCtDef &ctdef,
                                 ObDASDMLBaseRtDef &rtdef,
                                 const ObDASTabletLoc *tablet_loc,
                                 ObDMLRtCtx &dml_rtctx,
                                 const ExprFixedArray &row,
                                 ObExpr *trans_info_expr, // debug info for 4377 defensive check
                                 ObChunkDatumStore::StoredRow *&stored_row);
  template <typename T>
  static const ObDASTableLocMeta *get_table_loc_meta(const T *multi_ctdef);
  static int check_nested_sql_legality(ObExecContext &ctx, common::ObTableID ref_table_id);
  static int add_related_index_info(const ObDASTabletLoc &tablet_loc,
                                    const DASDMLCtDefArray &related_ctdefs,
                                    DASDMLRtDefArray &related_rtdefs,
                                    ObIDASTaskOp &das_op);
  static int convert_exprs_to_row(const ExprFixedArray &exprs,
                                  ObEvalCtx &eval_ctx,
                                  ObDMLBaseRtDef &dml_rtdef,
                                  common::ObIAllocator &allocator);
  static int build_batch_fk_check_tasks(const ObDMLBaseCtDef &dml_ctdef,
                                        ObDMLBaseRtDef &dml_rtdef);
};

template <int N, typename DMLIterator>
class ObDASIndexDMLAdaptor
{
  typedef typename das_reg::ObDASOpTypeTraits<N>::DASCtDef CtDefType;
  typedef typename das_reg::ObDASOpTypeTraits<N>::DASRtDef RtDefType;
  typedef typename das_reg::ObDASOpTypeTraits<N>::DASOp OpType;
public:
  ObDASIndexDMLAdaptor()
    : tx_desc_(nullptr),
      snapshot_(nullptr),
      write_branch_id_(0),
      ctdef_(nullptr),
      rtdef_(nullptr),
      related_ctdefs_(nullptr),
      related_rtdefs_(nullptr),
      tablet_id_(),
      ls_id_(),
      related_tablet_ids_(nullptr),
      das_allocator_(nullptr),
      ft_doc_word_infos_(nullptr),
      vec_index_acquire_ctxs_(nullptr),
      is_do_gts_opt_(false),
      tablet_loc_(nullptr),
      dml_param_()
  { }
  int write_tablet(DMLIterator &iter, int64_t &affected_rows);
  void set_outtable_lob_control_keys(
      common::ObIArray<storage::ObOuttableLobControlKey> *outtable_lob_control_keys)
  {
    dml_param_.outtable_lob_control_keys_ = outtable_lob_control_keys;
  }
  // The main iterator invokes this after storage's write check and before
  // returning its first row. Auxiliary rewinds reuse the completed batch.
  // Regular and conflict-fetch INSERT must share this entry point.
  // SPFresh counter/posting updates intentionally retain legacy expression
  // semantics: they are best-effort, per-write-attempt in-memory scheduling
  // hints, not transactional state, and are rebuilt from persisted rows by
  // PREWARM. Rollback compensation is not implemented here; transactional
  // cache-count consistency is a separate follow-up.
  int materialize_logical_ivf_rows(ObDASWriteBuffer &buffer);
  static int materialize_ivf_callback(void *ctx, ObDASWriteBuffer &buffer)
  {
    return static_cast<ObDASIndexDMLAdaptor *>(ctx)->materialize_logical_ivf_rows(buffer);
  }
  int write_rows(const share::ObLSID &ls_id,
                 const common::ObTabletID &tablet_id,
                 const CtDefType &ctdef,
                 RtDefType &rtdef,
                 DMLIterator &iter,
                 int64_t &affected_rows);
  int write_tablet_with_ignore(DMLIterator &iter, int64_t &affected_rows);
public:
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  int16_t write_branch_id_;
  const CtDefType *ctdef_;
  RtDefType *rtdef_;
  const DASCtDefFixedArray *related_ctdefs_;
  DASRtDefFixedArray *related_rtdefs_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  ObTabletIDFixedArray *related_tablet_ids_;
  common::ObIAllocator *das_allocator_;
  common::ObIArray<ObFTDocWordInfo> *ft_doc_word_infos_;
  common::ObIArray<share::ObVectorIndexAcquireCtx> *vec_index_acquire_ctxs_;
  bool is_do_gts_opt_;
  // M6: main op's tablet_loc, used to resolve physical KV tablet via Path A
  // (ObDASUtils::get_related_tablet_loc) for the entire op (main + aux). Only
  // the main IndexDMLInfo registers physical_data_table_id_ in
  // loc_meta_->related_table_ids_, so the related-loop ignores its tablet_id
  // input and always resolves via this field.
  const ObDASTabletLoc *tablet_loc_;
private:
  storage::ObDMLBaseParam dml_param_;
};

template <int N, typename DMLIterator>
int ObDASIndexDMLAdaptor<N, DMLIterator>::materialize_logical_ivf_rows(ObDASWriteBuffer &buffer)
{
  int ret = common::OB_SUCCESS;
  bool has_ivf_rows = false;
  // Related CtDef, RtDef, and tablet arrays describe the same auxiliary table
  // at each index. Reject deserialization/alignment damage before any write.
  if (OB_NOT_NULL(related_ctdefs_) && !related_ctdefs_->empty()
      && (OB_ISNULL(related_rtdefs_) || OB_ISNULL(related_tablet_ids_)
          || related_ctdefs_->count() != related_rtdefs_->count()
          || related_ctdefs_->count() != related_tablet_ids_->count())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_DAS_LOG(WARN, "related CtDef/RtDef/tablet arrays are not aligned",
                K(ret), "ctdef_count", related_ctdefs_->count(),
                "rtdef_count", OB_ISNULL(related_rtdefs_)
                    ? OB_INVALID_COUNT : related_rtdefs_->count(),
                "tablet_count", OB_ISNULL(related_tablet_ids_)
                    ? OB_INVALID_COUNT : related_tablet_ids_->count());
  } else if (OB_NOT_NULL(ctdef_) && OB_NOT_NULL(rtdef_)
      && ctdef_->is_logical_table() && OB_NOT_NULL(related_ctdefs_)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs_->count(); ++i) {
      const CtDefType *ivf_ctdef =
          static_cast<const CtDefType *>(related_ctdefs_->at(i));
      bool duplicate_assignment = false;
      if (OB_ISNULL(ivf_ctdef)
          || ObIVFDMLCtDef::DISABLED_PROTOCOL_VERSION
              == ivf_ctdef->ivf_dml_ctdef_.protocol_version_) {
        // legacy/non-IVF related table
      } else if (OB_UNLIKELY(!ivf_ctdef->ivf_dml_ctdef_.is_valid())) {
        ret = OB_NOT_SUPPORTED;
        SQL_DAS_LOG(WARN, "unsupported or incomplete IVF CtDef protocol",
                    K(ret), K(ivf_ctdef->ivf_dml_ctdef_));
      } else if (OB_UNLIKELY(!ivf_ctdef->is_logical_table()
          || !share::is_ivf_user_row_aux_type(
              ivf_ctdef->logical_table_aux_type_))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_DAS_LOG(WARN, "IVF CtDef metadata escaped logical-table path",
                    K(ret), KPC(ivf_ctdef));
      } else {
        for (int64_t j = 0; !duplicate_assignment && j < i; ++j) {
          const CtDefType *previous =
              static_cast<const CtDefType *>(related_ctdefs_->at(j));
          if (OB_NOT_NULL(previous)
              && previous->ivf_dml_ctdef_.is_valid()
              && previous->ivf_dml_ctdef_.cid_store_idx_
                  == ivf_ctdef->ivf_dml_ctdef_.cid_store_idx_) {
            if (OB_UNLIKELY(previous->ivf_dml_ctdef_.centroid_table_id_
                    != ivf_ctdef->ivf_dml_ctdef_.centroid_table_id_
                || previous->ivf_dml_ctdef_.vector_store_idx_
                    != ivf_ctdef->ivf_dml_ctdef_.vector_store_idx_
                || previous->ivf_dml_ctdef_.vector_subschema_id_
                    != ivf_ctdef->ivf_dml_ctdef_.vector_subschema_id_
                || previous->ivf_dml_ctdef_.distance_algorithm_
                    != ivf_ctdef->ivf_dml_ctdef_.distance_algorithm_)) {
              ret = OB_ERR_UNEXPECTED;
              SQL_DAS_LOG(WARN, "same IVF CID slot has inconsistent assignment metadata",
                          K(ret), K(previous->ivf_dml_ctdef_),
                          K(ivf_ctdef->ivf_dml_ctdef_));
            } else {
              duplicate_assignment = true;
            }
          }
        }
        const share::schema::ObTableSchemaParam &ivf_table =
            ivf_ctdef->table_param_.get_data_table();
        if (OB_FAIL(ret)) {
        } else if (duplicate_assignment) {
          // Siblings share one StoredRow CID slot; never assign it twice.
        } else if (share::schema::is_vec_ivfpq_code_index(ivf_table.get_index_type())
            || share::schema::is_vec_ivfpq_rowkey_cid_index(ivf_table.get_index_type())
            || share::schema::is_vec_ivfsq8_index(ivf_table.get_index_type())) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "logical-table non-FLAT IVF DML");
          SQL_DAS_LOG(WARN, "logical-table IVF SQ8/PQ DML is reserved",
                      K(ret), K(ivf_table.get_index_type()));
        } else {
          ObTabletID centroid_tablet_id;
          if (OB_FAIL(resolve_ivf_centroid_tablet(
              *related_ctdefs_, *related_tablet_ids_,
              ivf_ctdef->ivf_dml_ctdef_.centroid_table_id_,
              centroid_tablet_id))) {
            SQL_DAS_LOG(WARN, "centroid related tablet is absent; main tablet fallback is forbidden",
                        K(ret), K(ivf_ctdef->ivf_dml_ctdef_), K(tablet_id_));
          }
          has_ivf_rows = buffer.get_row_cnt() > 0;
          if (OB_SUCC(ret) && OB_FAIL(buffer.materialize_ivf_rows(
              *ivf_ctdef, centroid_tablet_id, rtdef_->lt_rt_, rtdef_->eval_ctx_))) {
            SQL_DAS_LOG(WARN, "materialize IVF rows directly from CtDef failed",
                        K(ret), K(ls_id_), K(centroid_tablet_id), KPC(ivf_ctdef));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && has_ivf_rows && OB_UNLIKELY(GCONF.is_debug_sync_enabled())) {
    // The lazy materializer has encoded every CID before yielding the first row to storage.
    (void)GDS.execute(common::AFTER_IVF_LOGIC_DML_CALC_CID_BEFORE_WRITE,
        MTL_ID(), ls_id_.id(), tablet_id_.id(), ctdef_->index_tid_);
  }
  return ret;
}

template <int N, typename DMLIterator>
int ObDASIndexDMLAdaptor<N, DMLIterator>::write_tablet(DMLIterator &iter, int64_t &affected_rows)
{
  ObDASWriteBuffer::IvfMaterializationGuard ivf_guard(
      iter.get_write_buffer(), this, &materialize_ivf_callback);
  int ret = common::OB_SUCCESS;
  affected_rows = 0;
  SQL_DAS_LOG(DEBUG, "begin to write the main tablet",
              K(ls_id_), K(tablet_id_), K(ctdef_->table_id_), K(ctdef_->index_tid_));
  if (!ivf_guard.is_registered()) {
    ret = OB_STATE_NOT_MATCH;
  } else if (ctdef_->is_ignore_ && ctdef_->is_logical_table()) {
    ret = OB_NOT_SUPPORTED;
    SQL_DAS_LOG(WARN, "logical table dml should not enter write tablet with ignore",
                K(ret), KPC_(ctdef));
  } else if (ctdef_->is_ignore_) {
    if (OB_FAIL(write_tablet_with_ignore(iter, affected_rows))) {
      SQL_DAS_LOG(WARN, "write tablet with ignore failed", K(ret));
    }
  } else {
    ObAccessService *as = MTL(ObAccessService *);
    storage::ObStoreCtxGuard store_ctx_guard;
    concurrent_control::ObWriteFlag write_flag;

    // write_flag should be inited before get store ctx, as it will be used in the call function
    (void)ObDMLService::init_dml_write_flag(*ctdef_, *rtdef_, write_flag, is_do_gts_opt_);
    if (OB_FAIL(as->get_write_store_ctx_guard(ls_id_,
                                              rtdef_->timeout_ts_,
                                              *tx_desc_,
                                              *snapshot_,
                                              write_branch_id_,
                                              write_flag,
                                              store_ctx_guard))) {
      SQL_DAS_LOG(WARN, "fail to get_write_store_ctx_guard", K(ret), K(ls_id_));
    } else {
      if (ctdef_->is_logical_table() && rtdef_->lt_rt_.enabled_) {
        iter.set_lt_rt(&rtdef_->lt_rt_);
        // Propagate session ns/lt to related logical aux rtdefs so init_dml_param and
        // logical-table write paths see enabled lt_rt_ (same as ensure_lt_rt_before_remote_rpc).
        if (related_ctdefs_ != nullptr && related_rtdefs_ != nullptr) {
          for (int64_t i = 0; i < related_ctdefs_->count(); ++i) {
            const CtDefType *related_ctdef = static_cast<const CtDefType *>(related_ctdefs_->at(i));
            RtDefType *related_rtdef = static_cast<RtDefType *>(related_rtdefs_->at(i));
            if (OB_NOT_NULL(related_ctdef) && OB_NOT_NULL(related_rtdef)
                && related_ctdef->is_logical_table()) {
              ObDASUtils::copy_lt_rt_from_main(rtdef_->lt_rt_, related_rtdef->lt_rt_);
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObDMLService::init_dml_param(
        *ctdef_, *rtdef_, *snapshot_, write_branch_id_, *das_allocator_,
        store_ctx_guard, dml_param_, is_do_gts_opt_, tablet_loc_,
        tablet_loc_ == nullptr ? &tablet_id_ : nullptr))) {
        SQL_DAS_LOG(WARN, "init dml param failed", K(ret), K(ctdef_->table_id_), K(ctdef_->index_tid_));
      } else if (FALSE_IT(dml_param_.vec_index_acquire_ctxs_ = vec_index_acquire_ctxs_)) {
      } else {
        ret = write_rows(ls_id_, tablet_id_, *ctdef_, *rtdef_, iter, affected_rows);
        if (dml_param_.ctx_.is_valid()) {
          dml_param_.table_param_ = &ctdef_->table_param_;
          dml_param_.ctx_.reset();
        }
        if (OB_FAIL(ret)) {
          SQL_DAS_LOG(WARN, "write rows failed", K(ret),
                      K(ls_id_), K(tablet_id_), K(ctdef_->table_id_), K(ctdef_->index_tid_));
        }
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (related_ctdefs_ != nullptr && !related_ctdefs_->empty()) {
      //write local index
      for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs_->count(); ++i) {
        const CtDefType *related_ctdef = static_cast<const CtDefType*>(related_ctdefs_->at(i));
        RtDefType *related_rtdef = static_cast<RtDefType*>(related_rtdefs_->at(i));
        ObTabletID related_tablet_id = related_tablet_ids_->at(i);
        ObFTDocWordInfo *doc_word_info = nullptr == ft_doc_word_infos_ ? nullptr : &(ft_doc_word_infos_->at(i));
        int64_t index_affected_rows = 0;
        SQL_DAS_LOG(DEBUG, "rewind iterator and write local index tablet",
                    K(ls_id_), K(related_tablet_id), K(related_ctdef->table_id_), K(related_ctdef->index_tid_),
                    KPC(doc_word_info));
        if (OB_FAIL(iter.rewind(related_ctdef, doc_word_info))) {
          SQL_DAS_LOG(WARN, "rewind iterator failed", K(ret));
        } else {
          if (related_ctdef->is_logical_table() && related_rtdef->lt_rt_.enabled_) {
            iter.set_lt_rt(&related_rtdef->lt_rt_);
          }
          if (OB_FAIL(ObDMLService::init_dml_param(*related_ctdef, *related_rtdef,
            *snapshot_, write_branch_id_, *das_allocator_, store_ctx_guard, dml_param_,
            is_do_gts_opt_, tablet_loc_,
            tablet_loc_ == nullptr ? &tablet_id_ : nullptr))) {
          SQL_DAS_LOG(WARN, "init index dml param failed", K(ret),
                      K(related_ctdef->table_id_), K(related_ctdef->index_tid_));
        } else {
          ret = write_rows(ls_id_,
                           related_tablet_id,
                           *related_ctdef,
                           *related_rtdef,
                           iter,
                           index_affected_rows);
          if (dml_param_.ctx_.is_valid()) {
            dml_param_.table_param_ = &related_ctdef->table_param_;
            dml_param_.ctx_.reset();
          }
          if (OB_FAIL(ret)) {
            SQL_DAS_LOG(WARN, "write local index rows failed", K(ret),
                     K(related_tablet_id), K(related_ctdef->table_id_), K(related_ctdef->index_tid_));
          }
        }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(ObDMLService::check_local_index_affected_rows(affected_rows,
                                                                         index_affected_rows,
                                                                         *ctdef_,
                                                                         *rtdef_,
                                                                         *related_ctdef,
                                                                         *related_rtdef))) {
          SQL_DAS_LOG(WARN, "check local index affected rows failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(GCONF.is_debug_sync_enabled())
      && ctdef_->is_logical_table() && OB_NOT_NULL(related_ctdefs_)) {
    bool has_ivf_index = false;
    for (int64_t i = 0; !has_ivf_index && i < related_ctdefs_->count(); ++i) {
      const CtDefType *related_ctdef = static_cast<const CtDefType *>(related_ctdefs_->at(i));
      has_ivf_index = OB_NOT_NULL(related_ctdef) && related_ctdef->is_logical_table()
          && share::schema::is_vec_ivfflat_cid_vector_index(
              related_ctdef->table_param_.get_data_table().get_index_type());
    }
    if (has_ivf_index) {
      // All main/auxiliary writes of this DAS batch are complete. This is the
      // single-tablet autocommit test window, not a general SQL COMMIT hook.
      (void)GDS.execute(common::BEFORE_IVF_LOGIC_DML_TX_COMMIT,
          MTL_ID(), ls_id_.id(), tablet_id_.id(), ctdef_->index_tid_);
    }
  }
  return ret;
}

template <int N, typename DMLIterator>
int ObDASIndexDMLAdaptor<N, DMLIterator>::write_tablet_with_ignore(DMLIterator &iter,
                                                                   int64_t &affected_rows)
{
  int ret = common::OB_SUCCESS;
  affected_rows = 0;
  const ObDASWriteBuffer::DmlRow *dml_row = nullptr;
  ObDASWriteBuffer::Iterator write_iter;
  ObAccessService *as = MTL(ObAccessService *);
  const bool with_local_index = related_ctdefs_ != nullptr && !related_ctdefs_->empty();
  if (OB_FAIL(iter.get_write_buffer().begin(write_iter))) {
    SQL_DAS_LOG(WARN, "begin write iterator failed", K(ret));
  }
  while (OB_SUCC(ret) && OB_SUCC(write_iter.get_next_row(dml_row))) {
    transaction::ObTxSEQ savepoint_no;
    int64_t table_affected_rows = 0;
    const int64_t control_key_count = OB_ISNULL(dml_param_.outtable_lob_control_keys_)
        ? 0 : dml_param_.outtable_lob_control_keys_->count();
    ObDASWriteBuffer single_row_buffer;
    ObDASWriteBuffer::DmlShadowRow dsr;
    bool added = false;
    //这里后续要删掉，临时处理
    ObChunkDatumStore::StoredRow *store_row = nullptr;
    dsr.store_row_ = const_cast<ObDASWriteBuffer::DmlRow*>(dml_row);
    if (OB_FAIL(ObDMLService::create_anonymous_savepoint(*tx_desc_, savepoint_no, write_branch_id_))) {
      SQL_DAS_LOG(WARN, "create anonymous savepoint failed", K(ret));
    } else if (OB_FAIL(single_row_buffer.init(*das_allocator_, ObDASWriteBuffer::DAS_ROW_DEFAULT_EXTEND_SIZE, MTL_ID()))) {
      SQL_DAS_LOG(WARN, "init single row buffer failed", K(ret));
    } else if (OB_FAIL(single_row_buffer.try_add_row(dsr, das::OB_DAS_MAX_PACKET_SIZE, added, &store_row))) {
      SQL_DAS_LOG(WARN, "try add row to single row buffer failed", K(ret));
    } else if (!added) {
      ret = OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "add row to single row buffer failed", K(ret));
    } else {
      SQL_DAS_LOG(TRACE, "write table dml row with ignore", KPC(dml_row), K(ls_id_), K(tablet_id_),
                  K(ctdef_->table_id_), K(ctdef_->index_tid_));
      DMLIterator single_row_iter(ctdef_, single_row_buffer, *das_allocator_);
      storage::ObStoreCtxGuard store_ctx_guard;
      concurrent_control::ObWriteFlag write_flag;

      (void)ObDMLService::init_dml_write_flag(*ctdef_, *rtdef_, write_flag, is_do_gts_opt_);
      if (OB_FAIL(as->get_write_store_ctx_guard(ls_id_,
                                                rtdef_->timeout_ts_,
                                                *tx_desc_,
                                                *snapshot_,
                                                write_branch_id_,
                                                write_flag,
                                                store_ctx_guard))) {
        SQL_DAS_LOG(WARN, "fail to get_write_store_ctx_guard", K(ret), K(ls_id_));
      } else if (OB_FAIL(ObDMLService::init_dml_param(*ctdef_, *rtdef_, *snapshot_, write_branch_id_,
          *das_allocator_, store_ctx_guard, dml_param_, is_do_gts_opt_))) {
        SQL_DAS_LOG(WARN, "init dml param failed", K(ret), KPC_(ctdef), KPC_(rtdef));
      } else if (FALSE_IT(dml_param_.vec_index_acquire_ctxs_ = vec_index_acquire_ctxs_)) {
      } else if (with_local_index && FALSE_IT(dml_param_.write_flag_.set_skip_flush_redo())) {
      } else if (OB_FAIL(write_rows(ls_id_,
                                    tablet_id_,
                                    *ctdef_,
                                    *rtdef_,
                                    single_row_iter,
                                    table_affected_rows))) {
        SQL_DAS_LOG(WARN, "write rows failed", K(ret), K(ls_id_), K(tablet_id_), KPC(ctdef_), KPC(rtdef_));
      } else if (with_local_index) {
        //write local index
        for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs_->count(); ++i) {
          const CtDefType *related_ctdef = static_cast<const CtDefType*>(related_ctdefs_->at(i));
          RtDefType *related_rtdef = static_cast<RtDefType*>(related_rtdefs_->at(i));
          ObFTDocWordInfo *doc_word_info = nullptr == ft_doc_word_infos_ ? nullptr : &(ft_doc_word_infos_->at(i));
          ObTabletID related_tablet_id = related_tablet_ids_->at(i);
          int64_t index_affected_rows = 0;
          SQL_DAS_LOG(TRACE, "rewind and write index dml row with ignore", KPC(dml_row),
                      K(ls_id_), K(related_tablet_id),
                      K(related_ctdef->table_id_), K(related_ctdef->index_tid_));
          if (OB_FAIL(single_row_iter.rewind(related_ctdef, doc_word_info))) {
            SQL_DAS_LOG(WARN, "rewind iterator failed", K(ret));
          } else {
            if (related_ctdef->is_logical_table() && related_rtdef->lt_rt_.enabled_) {
              single_row_iter.set_lt_rt(&related_rtdef->lt_rt_);
            }
            if (OB_FAIL(ObDMLService::init_dml_param(*related_ctdef,
                                                          *related_rtdef,
                                                          *snapshot_,
                                                          write_branch_id_,
                                                          *das_allocator_,
                                                          store_ctx_guard,
                                                          dml_param_,
                                                          is_do_gts_opt_))) {
            SQL_DAS_LOG(WARN, "init index dml param failed", K(ret),
                        KPC(related_ctdef), KPC(related_rtdef));
          } else if (i == related_ctdefs_->count() - 1 && FALSE_IT(dml_param_.write_flag_.unset_skip_flush_redo())) {
          } else if (OB_FAIL(write_rows(ls_id_,
                                        related_tablet_id,
                                        *related_ctdef,
                                        *related_rtdef,
                                        single_row_iter,
                                        index_affected_rows))) {
            SQL_DAS_LOG(WARN, "write local index rows failed", K(ret),
                     K(related_tablet_id), K(related_ctdef->table_id_), K(related_ctdef->index_tid_));
          } else if (OB_FAIL(ObDMLService::check_local_index_affected_rows(table_affected_rows,
                                                                           index_affected_rows,
                                                                           *ctdef_,
                                                                           *rtdef_,
                                                                           *related_ctdef,
                                                                           *related_rtdef))) {
            SQL_DAS_LOG(WARN, "check local index affected rows failed", K(ret));
          }
          }
        }
      }
    }
    if (common::OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      //rollback to savepoint
      if (OB_FAIL(ObDMLService::rollback_local_savepoint(*tx_desc_,
                                                         savepoint_no,
                                                         dml_param_.timeout_))) {
        SQL_DAS_LOG(WARN, "rollbakc to local savepoint failed", K(ret));
      } else if (OB_NOT_NULL(dml_param_.outtable_lob_control_keys_)) {
        while (dml_param_.outtable_lob_control_keys_->count() > control_key_count) {
          dml_param_.outtable_lob_control_keys_->pop_back();
        }
      }
    } else if (OB_SUCC(ret)) {
      ++affected_rows;
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

template <int N, typename DMLIterator>
int ObDASIndexDMLAdaptor<N, DMLIterator>::write_rows(const share::ObLSID &ls_id,
                                                     const common::ObTabletID &tablet_id,
                                                     const CtDefType &ctdef,
                                                     RtDefType &rtdef,
                                                     DMLIterator &iter,
                                                     int64_t &affected_rows)
{
  UNUSEDx(ls_id, tablet_id, ctdef, rtdef, iter, affected_rows);
  return common::OB_NOT_IMPLEMENT;
}
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_ENGINE_DML_OB_DML_SERVICE_H_ */
