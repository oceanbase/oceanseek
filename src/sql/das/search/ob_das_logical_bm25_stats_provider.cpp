/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_logical_bm25_stats_provider.h"
#include "sql/das/search/ob_das_logical_bm25_scan_host.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/search/ob_das_search_utils.h"

namespace oceanbase
{
namespace sql
{

ObLogicalFilteredBM25StatsProvider::ObLogicalFilteredBM25StatsProvider()
  : key_(),
    primary_stats_ctdef_(nullptr),
    primary_stats_rtdef_(nullptr),
    inv_scan_ctdef_(nullptr),
    inv_scan_rtdef_(nullptr),
    filter_exprs_(nullptr),
    host_(nullptr),
    stats_(),
    filtered_doc_ids_(),
    doc_len_seen_ids_(),
    primary_scan_param_(),
    primary_scan_iter_(),
    primary_tablet_id_(),
    primary_scan_inited_(false),
    inv_scan_param_(),
    inv_scan_iter_(),
    inv_tablet_id_(),
    inv_scan_inited_(false),
    doc_len_range_objs_(),
    token_range_objs_(),
    filtered_doc_set_built_(false),
    doc_length_stats_built_(false),
    use_scalar_ctdef_(false),
    is_inited_(false)
{}

void ObLogicalFilteredBM25StatsProvider::reset()
{
  // Release any iter still inited (caller exception path safety). host_ is
  // required by init_and_open_scalar_iter_ for tsc_service routing, but
  // ObDASScanIter::release does not touch host_, so direct release is safe
  // even after the caller dropped host_.
  (void)release_scalar_iter_(primary_scan_param_, primary_scan_iter_, primary_scan_inited_);
  (void)release_scalar_iter_(inv_scan_param_, inv_scan_iter_, inv_scan_inited_);
  key_ = ObLogicalFilteredBM25StatsKey();
  primary_stats_ctdef_ = nullptr;
  primary_stats_rtdef_ = nullptr;
  inv_scan_ctdef_ = nullptr;
  inv_scan_rtdef_ = nullptr;
  filter_exprs_ = nullptr;
  host_ = nullptr;
  stats_ = ObLogicalFilteredBM25Stats();
  filtered_doc_ids_.destroy();
  doc_len_seen_ids_.destroy();
  primary_tablet_id_.reset();
  inv_tablet_id_.reset();
  filtered_doc_set_built_ = false;
  doc_length_stats_built_ = false;
  use_scalar_ctdef_ = false;
  is_inited_ = false;
}

int ObLogicalFilteredBM25StatsProvider::init(
    const ObLogicalFilteredBM25StatsKey &key,
    const ObDASBaseCtDef *primary_stats_ctdef,
    ObDASBaseRtDef *primary_stats_rtdef,
    const ObDASBaseCtDef *inv_scan_ctdef,
    ObDASBaseRtDef *inv_scan_rtdef,
    const sql::ExprFixedArray *filter_exprs,
    bool use_scalar_ctdef,
    ObILogicalBm25ScanHost *host)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("filtered bm25 stats provider double init", K(ret));
  } else if (OB_ISNULL(host)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scan host is null", K(ret));
  } else if (OB_FAIL(filtered_doc_ids_.create(1024, "LtBM25DocSet", "LtBM25DocNode", MTL_ID()))) {
    LOG_WARN("failed to create filtered doc id set", K(ret));
  } else if (OB_FAIL(doc_len_seen_ids_.create(1024, "LtBM25LenSet", "LtBM25LenNode", MTL_ID()))) {
    LOG_WARN("failed to create doc length seen set", K(ret));
  } else {
    key_ = key;
    primary_stats_ctdef_ = primary_stats_ctdef;
    primary_stats_rtdef_ = primary_stats_rtdef;
    inv_scan_ctdef_ = inv_scan_ctdef;
    inv_scan_rtdef_ = inv_scan_rtdef;
    filter_exprs_ = filter_exprs;
    host_ = host;
    use_scalar_ctdef_ = use_scalar_ctdef;
    is_inited_ = true;
  }
  return ret;
}

common::ObTableID ObLogicalFilteredBM25StatsProvider::get_ref_table_id_(
    const ObDASBaseCtDef *ctdef) const
{
  return use_scalar_ctdef_
      ? static_cast<const ObDASScalarScanCtDef *>(ctdef)->ref_table_id_
      : static_cast<const ObDASScanCtDef *>(ctdef)->ref_table_id_;
}

const sql::ExprFixedArray &ObLogicalFilteredBM25StatsProvider::get_rowkey_exprs_(
    const ObDASBaseCtDef *ctdef) const
{
  return use_scalar_ctdef_
      ? static_cast<const ObDASScalarScanCtDef *>(ctdef)->rowkey_exprs_
      : static_cast<const ObDASScanCtDef *>(ctdef)->rowkey_exprs_;
}

const sql::ExprFixedArray &ObLogicalFilteredBM25StatsProvider::get_logical_output_exprs_(
    const ObDASBaseCtDef *ctdef) const
{
  return use_scalar_ctdef_
      ? static_cast<const ObDASScalarScanCtDef *>(ctdef)->logical_output_exprs_
      : static_cast<const ObDASScanCtDef *>(ctdef)->logical_output_exprs_;
}

const sql::ExprFixedArray &ObLogicalFilteredBM25StatsProvider::get_result_output_(
    const ObDASBaseCtDef *ctdef) const
{
  return use_scalar_ctdef_
      ? static_cast<const ObDASScalarScanCtDef *>(ctdef)->result_output_
      : static_cast<const ObDASScanCtDef *>(ctdef)->result_output_;
}

int ObLogicalFilteredBM25StatsProvider::init_and_open_scalar_iter_(
    const ObDASBaseCtDef *ctdef,
    ObDASBaseRtDef *rtdef,
    storage::ObTableScanParam &scan_param,
    ObDASScanIter &iter,
    common::ObTabletID &tablet_id,
    bool &inited)
{
  int ret = OB_SUCCESS;
  bool scan_param_inited = false;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef) || OB_ISNULL(host_)
      || OB_ISNULL(rtdef->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret), KP(ctdef), KP(rtdef), KP(host_));
  } else {
    const ObLogicalTableSearchCtx &lt_ctx = host_->get_logical_table_ctx();
    if (ctdef->is_logical_table() && lt_ctx.is_enabled_
        && share::LT_AUX_PRIMARY == ctdef->logical_table_aux_type_) {
      if (OB_FAIL(host_->get_related_tablet_id(ctdef, tablet_id))) {
        LOG_WARN("failed to get primary aux tablet id for bm25 stats scan", K(ret));
      }
    } else if (ctdef->is_logical_table() && lt_ctx.is_enabled_) {
      tablet_id = lt_ctx.physical_tablet_id_;
      if (!tablet_id.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LT hybrid BM25] physical_tablet_id_ invalid",
                 K(ret), K(tablet_id), "aux_type", static_cast<int>(ctdef->logical_table_aux_type_));
      }
    } else if (OB_FAIL(host_->get_related_tablet_id(ctdef, tablet_id))) {
      LOG_WARN("failed to get related tablet id for bm25 stats scan", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(host_->init_scan_param(tablet_id, ctdef, rtdef, scan_param))) {
      LOG_WARN("failed to init bm25 stats scan param", K(ret), K(tablet_id));
    } else {
      scan_param_inited = true;
      if (ctdef->is_logical_table() && lt_ctx.is_enabled_) {
        scan_param.index_id_ = lt_ctx.physical_data_table_id_;
      }
      ObDASScanIterParam iter_param;
      if (use_scalar_ctdef_) {
        iter_param.scan_ctdef_ = nullptr;
        iter_param.scalar_scan_ctdef_ = static_cast<const ObDASScalarScanCtDef *>(ctdef);
        iter_param.use_scalar_ctdef_ = true;
      } else {
        iter_param.scan_ctdef_ = static_cast<const ObDASScanCtDef *>(ctdef);
        iter_param.scalar_scan_ctdef_ = nullptr;
        iter_param.use_scalar_ctdef_ = false;
      }
      iter_param.max_size_ = rtdef->eval_ctx_->is_vectorized()
                                 ? rtdef->eval_ctx_->max_batch_size_ : 1;
      iter_param.eval_ctx_ = rtdef->eval_ctx_;
      iter_param.exec_ctx_ = &rtdef->eval_ctx_->exec_ctx_;
      iter_param.output_ = &get_result_output_(ctdef);
      if (ctdef->is_logical_table() && lt_ctx.is_enabled_) {
        iter_param.self_managed_logical_lt_ctx_ = true;
        iter_param.namespace_id_ = lt_ctx.namespace_id_;
        iter_param.ltable_id_ = lt_ctx.logical_table_id_;
      }
      if (OB_FAIL(iter.init(iter_param))) {
        LOG_WARN("failed to init bm25 stats scan iter", K(ret));
      } else {
        iter.set_scan_param(scan_param);
        inited = true;
      }
    }
  }
  if (OB_FAIL(ret) && scan_param_inited && !inited) {
    scan_param.destroy_schema_guard();
    scan_param.snapshot_.reset();
    scan_param.destroy();
  }
  return ret;
}

int ObLogicalFilteredBM25StatsProvider::release_scalar_iter_(
    storage::ObTableScanParam &scan_param,
    ObDASScanIter &iter,
    bool &inited)
{
  int ret = OB_SUCCESS;
  if (inited) {
    int release_ret = iter.release();
    if (OB_SUCCESS != release_ret) {
      LOG_WARN("failed to release bm25 stats scan iter", K(release_ret));
      ret = release_ret;
    }
    scan_param.destroy_schema_guard();
    scan_param.snapshot_.reset();
    scan_param.destroy();
    inited = false;
  }
  return ret;
}

int ObLogicalFilteredBM25StatsProvider::build_filtered_doc_set_once_()
{
  int ret = OB_SUCCESS;
  if (filtered_doc_set_built_) {
    // skip
  } else if (OB_ISNULL(primary_stats_ctdef_) || OB_ISNULL(primary_stats_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logical filtered bm25 stats provider missing primary scan ctdef",
             K(ret), K(key_));
  } else if (OB_ISNULL(host_) || OB_ISNULL(host_->get_eval_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("host or eval ctx is null", K(ret));
  } else {
    const int64_t max_batch_size = OB_MAX(host_->get_eval_ctx()->max_batch_size_, 1);
    ObSEArray<ObObj, 8> start_objs;
    ObSEArray<ObObj, 8> end_objs;
    ObNewRange full_range;
    const sql::ExprFixedArray &primary_rowkey_exprs = get_rowkey_exprs_(primary_stats_ctdef_);
    const int64_t rowkey_cnt = primary_rowkey_exprs.count();
    if (OB_UNLIKELY(rowkey_cnt <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty primary stats rowkey exprs", K(ret));
    } else if (OB_FAIL(start_objs.prepare_allocate(rowkey_cnt))) {
      LOG_WARN("failed to alloc start objs", K(ret), K(rowkey_cnt));
    } else if (OB_FAIL(end_objs.prepare_allocate(rowkey_cnt))) {
      LOG_WARN("failed to alloc end objs", K(ret), K(rowkey_cnt));
    } else {
      for (int64_t i = 0; i < rowkey_cnt; ++i) {
        start_objs.at(i).set_min_value();
        end_objs.at(i).set_max_value();
      }
      full_range.table_id_ = get_ref_table_id_(primary_stats_ctdef_);
      full_range.start_key_.assign(start_objs.get_data(), rowkey_cnt);
      full_range.end_key_.assign(end_objs.get_data(), rowkey_cnt);
      full_range.border_flag_.set_inclusive_start();
      full_range.border_flag_.set_inclusive_end();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(init_and_open_scalar_iter_(primary_stats_ctdef_, primary_stats_rtdef_,
                                             primary_scan_param_, primary_scan_iter_,
                                             primary_tablet_id_, primary_scan_inited_))) {
        LOG_WARN("failed to init primary stats iter", K(ret));
      } else if (OB_FAIL(primary_scan_param_.key_ranges_.push_back(full_range))) {
        LOG_WARN("failed to push primary stats full range", K(ret), K(full_range));
      } else if (OB_FAIL(primary_scan_iter_.do_table_scan())) {
        LOG_WARN("failed to do table scan for primary stats", K(ret));
      } else {
        bool iter_end = false;
        while (OB_SUCC(ret) && !iter_end) {
          int64_t count = 0;
          ret = primary_scan_iter_.get_next_rows(count, max_batch_size);
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            iter_end = true;
          }
          ObEvalCtx *eval_ctx = host_->get_eval_ctx();
          ObExpr *rowid_expr = nullptr;
          if (OB_FAIL(ret) || 0 == count) {
            // skip
          } else {
            const ObLogicalTableColumnMapping &mapping =
                primary_stats_ctdef_->logical_column_mapping_;
            const ExprFixedArray &logical_output_exprs =
                get_logical_output_exprs_(primary_stats_ctdef_);
            for (int64_t mi = 0; OB_SUCC(ret) && mi < mapping.entries_.count(); ++mi) {
              if (FROM_ROWID == mapping.entries_.at(mi).source_) {
                if (mi >= logical_output_exprs.count()
                    || OB_ISNULL(logical_output_exprs.at(mi))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("missing rowid logical output expr for primary stats scan",
                           K(ret), K(mi), "logical_output_cnt", logical_output_exprs.count());
                } else {
                  rowid_expr = logical_output_exprs.at(mi);
                }
                break;
              }
            }
            if (OB_SUCC(ret) && OB_ISNULL(rowid_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("no FROM_ROWID mapping in stats primary ctdef", K(ret));
            }
          }
          if (OB_FAIL(ret) || 0 == count || OB_ISNULL(rowid_expr)) {
            // skip
          } else {
            ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
            guard.set_batch_size(count);
            for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              bool passed = true;
              guard.set_batch_idx(i);
              for (int64_t j = 0; OB_SUCC(ret) && passed && OB_NOT_NULL(filter_exprs_)
                   && j < filter_exprs_->count(); ++j) {
                ObExpr *filter = filter_exprs_->at(j);
                ObDatum *filter_datum = nullptr;
                if (OB_ISNULL(filter)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected null filter expr", K(ret), K(j));
                } else {
                  filter->clear_evaluated_flag(*eval_ctx);
                  if (OB_FAIL(filter->eval(*eval_ctx, filter_datum))) {
                    LOG_WARN("failed to eval logical bm25 filter", K(ret), K(j));
                  } else {
                    passed = OB_NOT_NULL(filter_datum) && filter_datum->is_true();
                  }
                }
              }
              if (OB_SUCC(ret) && passed) {
                uint64_t rowid = 0;
                if (OB_FAIL(ObDASSearchUtils::get_uint64_at(*rowid_expr, *eval_ctx, i, rowid))) {
                  LOG_WARN("failed to get rowid for filtered doc set", K(ret), K(i));
                } else {
                  int hash_ret = filtered_doc_ids_.set_refactored(rowid);
                  if (OB_SUCCESS == hash_ret) {
                    ++stats_.total_doc_cnt_;
                  } else if (OB_HASH_EXIST == hash_ret) {
                    // duplicate primary row should not happen, but keep count exact.
                  } else {
                    ret = hash_ret;
                    LOG_WARN("failed to add filtered rowid", K(ret), K(rowid));
                  }
                }
              }
            }
          }
        }
      }
    }
    int release_ret = release_scalar_iter_(primary_scan_param_, primary_scan_iter_, primary_scan_inited_);
    if (OB_SUCCESS != release_ret && OB_SUCC(ret)) {
      ret = release_ret;
    }
  }
  if (OB_SUCC(ret)) {
    filtered_doc_set_built_ = true;
  }
  return ret;
}

int ObLogicalFilteredBM25StatsProvider::build_doc_length_stats_once_()
{
  int ret = OB_SUCCESS;
  if (doc_length_stats_built_) {
    // skip
  } else if (OB_FAIL(build_filtered_doc_set_once_())) {
    LOG_WARN("failed to build filtered doc set", K(ret));
  } else if (0 == stats_.total_doc_cnt_) {
    stats_.avg_doc_token_cnt_ = 0.0;
  } else {
    // First logical FTS BM25 version reuses the inverted scan ctdef and deduplicates rowids.
    // This is exact but O(postings); a forward-aux scan can replace it later for O(docs).
    const int64_t max_batch_size = OB_MAX(host_->get_eval_ctx()->max_batch_size_, 1);
    uint64_t doc_len_sum = 0;
    ObNewRange full_range;
    if (OB_ISNULL(inv_scan_ctdef_) || OB_ISNULL(inv_scan_rtdef_)
        || OB_ISNULL(key_.ir_ctdef_) || OB_ISNULL(key_.ir_ctdef_->inv_scan_domain_id_col_)
        || OB_ISNULL(key_.ir_ctdef_->inv_scan_doc_length_col_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("missing inverted scan defs or exprs for logical bm25 doc length stats", K(ret));
    } else {
      doc_len_range_objs_[0].set_min_value();
      doc_len_range_objs_[1].set_min_value();
      doc_len_range_objs_[2].set_max_value();
      doc_len_range_objs_[3].set_max_value();
      full_range.table_id_ = get_ref_table_id_(inv_scan_ctdef_);
      full_range.start_key_.assign(doc_len_range_objs_, 2);
      full_range.end_key_.assign(doc_len_range_objs_ + 2, 2);
      full_range.border_flag_.set_inclusive_start();
      full_range.border_flag_.set_inclusive_end();
    }
    if (OB_SUCC(ret)) {
      inv_scan_param_.key_ranges_.reuse();
      if (OB_FAIL(init_and_open_scalar_iter_(inv_scan_ctdef_, inv_scan_rtdef_,
                                             inv_scan_param_, inv_scan_iter_,
                                             inv_tablet_id_, inv_scan_inited_))) {
        LOG_WARN("failed to init inv stats iter for doc length", K(ret));
      } else if (OB_FAIL(inv_scan_param_.key_ranges_.push_back(full_range))) {
        LOG_WARN("failed to push inverted full stats range", K(ret), K(full_range));
      } else if (OB_FAIL(inv_scan_iter_.do_table_scan())) {
        LOG_WARN("failed to do table scan for doc length stats", K(ret));
      } else {
        bool iter_end = false;
        ObEvalCtx *eval_ctx = host_->get_eval_ctx();
        ObExpr *rowid_expr = key_.ir_ctdef_->inv_scan_domain_id_col_;
        ObExpr *doc_len_expr = key_.ir_ctdef_->inv_scan_doc_length_col_;
        while (OB_SUCC(ret) && !iter_end) {
          int64_t count = 0;
          ret = inv_scan_iter_.get_next_rows(count, max_batch_size);
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            iter_end = true;
          }
          if (OB_SUCC(ret) && count > 0) {
            for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              uint64_t rowid = 0;
              uint64_t doc_len = 0;
              if (OB_FAIL(ObDASSearchUtils::get_uint64_at(*rowid_expr, *eval_ctx, i, rowid))) {
                LOG_WARN("failed to get rowid for doc length stats", K(ret), K(i));
              } else {
                int filtered_exist_ret = filtered_doc_ids_.exist_refactored(rowid);
                if (OB_HASH_NOT_EXIST == filtered_exist_ret) {
                  // skip rowids outside the filtered document set
                } else if (OB_HASH_EXIST != filtered_exist_ret) {
                  ret = filtered_exist_ret;
                  LOG_WARN("failed to check filtered rowid", K(ret), K(rowid));
                } else {
                  int seen_exist_ret = doc_len_seen_ids_.exist_refactored(rowid);
                  if (OB_HASH_EXIST == seen_exist_ret) {
                    // doc length is counted once per document
                  } else if (OB_HASH_NOT_EXIST != seen_exist_ret) {
                    ret = seen_exist_ret;
                    LOG_WARN("failed to check doc len seen rowid", K(ret), K(rowid));
                  } else {
                    int hash_ret = doc_len_seen_ids_.set_refactored(rowid);
                    if (OB_HASH_EXIST == hash_ret) {
                      // skip duplicate rowid observed between check and insert
                    } else if (OB_SUCCESS != hash_ret) {
                      ret = hash_ret;
                      LOG_WARN("failed to add doc len seen rowid", K(ret), K(rowid));
                    } else if (OB_FAIL(ObDASSearchUtils::get_uint64_at(*doc_len_expr, *eval_ctx, i, doc_len))) {
                      LOG_WARN("failed to get doc length for stats", K(ret), K(i), K(rowid));
                    } else {
                      doc_len_sum += doc_len;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    int release_ret = release_scalar_iter_(inv_scan_param_, inv_scan_iter_, inv_scan_inited_);
    if (OB_SUCCESS != release_ret && OB_SUCC(ret)) {
      ret = release_ret;
    }
    if (OB_SUCC(ret)) {
      stats_.avg_doc_token_cnt_ = static_cast<double>(doc_len_sum) / static_cast<double>(stats_.total_doc_cnt_);
    }
  }
  if (OB_SUCC(ret)) {
    doc_length_stats_built_ = true;
  }
  return ret;
}

int ObLogicalFilteredBM25StatsProvider::scan_inverted_token_filtered_(
    const ObString &token, int64_t &token_doc_cnt)
{
  int ret = OB_SUCCESS;
  token_doc_cnt = 0;
  if (OB_FAIL(build_filtered_doc_set_once_())) {
    LOG_WARN("failed to build filtered doc set", K(ret));
  } else {
    const int64_t max_batch_size = OB_MAX(host_->get_eval_ctx()->max_batch_size_, 1);
    if (OB_ISNULL(inv_scan_ctdef_) || OB_ISNULL(inv_scan_rtdef_)
        || OB_ISNULL(key_.ir_ctdef_) || OB_ISNULL(key_.ir_ctdef_->search_text_)
        || OB_ISNULL(key_.ir_ctdef_->inv_scan_domain_id_col_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("missing inverted scan defs or exprs for logical bm25 token stats", K(ret));
    } else {
      ObNewRange scan_range;
      token_range_objs_[0].set_common_value(token);
      token_range_objs_[0].set_meta_type(key_.ir_ctdef_->search_text_->obj_meta_);
      token_range_objs_[1].set_min_value();
      token_range_objs_[2].set_common_value(token);
      token_range_objs_[2].set_meta_type(key_.ir_ctdef_->search_text_->obj_meta_);
      token_range_objs_[3].set_max_value();
      scan_range.table_id_ = get_ref_table_id_(inv_scan_ctdef_);
      scan_range.start_key_.assign(token_range_objs_, 2);
      scan_range.end_key_.assign(token_range_objs_ + 2, 2);
      scan_range.border_flag_.set_inclusive_start();
      scan_range.border_flag_.set_inclusive_end();
      const bool first_call = !inv_scan_inited_;
      inv_scan_param_.key_ranges_.reuse();
      if (first_call) {
        if (OB_FAIL(init_and_open_scalar_iter_(inv_scan_ctdef_, inv_scan_rtdef_,
                                               inv_scan_param_, inv_scan_iter_,
                                               inv_tablet_id_, inv_scan_inited_))) {
          LOG_WARN("failed to init inv stats iter for token scan", K(ret), K(token));
        } else if (OB_FAIL(inv_scan_param_.key_ranges_.push_back(scan_range))) {
          LOG_WARN("failed to push token scan range", K(ret), K(token));
        } else if (OB_FAIL(inv_scan_iter_.do_table_scan())) {
          LOG_WARN("failed to do table scan for token stats", K(ret), K(token));
        }
      } else {
        // iter.reuse() resets scan_param.key_ranges_; push must come after.
        if (OB_FAIL(inv_scan_iter_.reuse())) {
          LOG_WARN("failed to reuse inv stats iter", K(ret), K(token));
        } else if (OB_FAIL(inv_scan_param_.key_ranges_.push_back(scan_range))) {
          LOG_WARN("failed to push token scan range on rescan", K(ret), K(token));
        } else if (OB_FAIL(inv_scan_iter_.rescan())) {
          LOG_WARN("failed to rescan inv stats iter", K(ret), K(token));
        }
      }
      if (OB_SUCC(ret)) {
        bool iter_end = false;
        ObEvalCtx *eval_ctx = host_->get_eval_ctx();
        ObExpr *rowid_expr = key_.ir_ctdef_->inv_scan_domain_id_col_;
        while (OB_SUCC(ret) && !iter_end) {
          int64_t count = 0;
          ret = inv_scan_iter_.get_next_rows(count, max_batch_size);
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            iter_end = true;
          }
          if (OB_SUCC(ret) && count > 0) {
            for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              uint64_t rowid = 0;
              if (OB_FAIL(ObDASSearchUtils::get_uint64_at(*rowid_expr, *eval_ctx, i, rowid))) {
                LOG_WARN("failed to get rowid for token stats", K(ret), K(i));
              } else {
                int exist_ret = filtered_doc_ids_.exist_refactored(rowid);
                if (OB_HASH_EXIST == exist_ret) {
                  ++token_doc_cnt;
                } else if (OB_HASH_NOT_EXIST == exist_ret) {
                  // skip rowids outside the filtered document set
                } else {
                  ret = exist_ret;
                  LOG_WARN("failed to check filtered rowid for token stats", K(ret), K(rowid));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLogicalFilteredBM25StatsProvider::get_total_doc_cnt_and_avg_doc_len(
    ObLogicalFilteredBM25Stats &stats)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard guard(*host_->get_eval_ctx());
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("filtered bm25 stats provider not initialized", K(ret));
  } else if (OB_FAIL(build_doc_length_stats_once_())) {
    LOG_WARN("failed to build doc length stats", K(ret));
  } else {
    stats = stats_;
  }
  return ret;
}

int ObLogicalFilteredBM25StatsProvider::get_token_doc_cnt(
    const ObString &token, int64_t &token_doc_cnt)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard guard(*host_->get_eval_ctx());
  token_doc_cnt = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("filtered bm25 stats provider not initialized", K(ret));
  } else if (OB_FAIL(scan_inverted_token_filtered_(token, token_doc_cnt))) {
    LOG_WARN("failed to scan inverted token filtered", K(ret), K(token));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
