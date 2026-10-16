/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_tr_merge_iter.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/das/ob_das_define.h"

namespace oceanbase
{
using namespace share;
namespace sql
{

ObDASTRMergeIter::ObDASTRMergeIter()
  : ObDASIter(ObDASIterType::DAS_ITER_TEXT_RETRIEVAL_MERGE),
    mem_context_(nullptr),
    myself_allocator_(lib::ObMemAttr(MTL_ID(), "SRMergeIterSelf"), OB_MALLOC_NORMAL_BLOCK_SIZE),
    ir_ctdef_(nullptr),
    ir_rtdef_(nullptr),
    tx_desc_(nullptr),
    snapshot_(nullptr),
    query_tokens_(),
    sr_iter_param_(),
    sparse_retrieval_iter_(nullptr),
    dim_iters_(),
    dim_iter_(),
    total_doc_cnt_scan_param_(nullptr),
    inv_scan_params_(),
    inv_agg_params_(),
    fwd_scan_params_(),
    boolean_compute_node_(nullptr),
    block_max_scan_params_(),
    block_max_iter_param_(),
    bm25_param_est_ctx_(),
    topk_limit_(0),
    ls_id_(),
    total_doc_cnt_tablet_id_(),
    inv_idx_tablet_id_(),
    fwd_idx_tablet_id_(),
    flags_(0),
    check_rangekey_inited_(false),
    inv_idx_tablet_switched_(false),
    is_inited_(false),
    logical_stats_provider_(nullptr),
    logical_table_ctx_(),
    runtime_ns_lt_loaded_(false)
{
}

int ObDASTRMergeIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(ObDASIterType::DAS_ITER_TEXT_RETRIEVAL_MERGE != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for text retrieval merge iter", K(ret), K(param));
  } else {
    ObDASTRMergeIterParam &merge_param = static_cast<ObDASTRMergeIterParam &>(param);
    ir_ctdef_ = merge_param.ir_ctdef_;
    ir_rtdef_ = merge_param.ir_rtdef_;
    tx_desc_ = merge_param.tx_desc_;
    snapshot_ = merge_param.snapshot_;
    boolean_compute_node_ = merge_param.boolean_compute_node_;
    flags_ = merge_param.flags_;
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_param.query_tokens_.count(); ++i) {
      if (OB_FAIL(query_tokens_.push_back(merge_param.query_tokens_.at(i)))) {
        LOG_WARN("failed to push back query token", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_rtdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null pointer", K(ret), KP_(ir_ctdef), KP_(ir_rtdef));
    } else if (topk_mode_ && OB_FAIL(init_topk_limit(ir_ctdef_, *ir_rtdef_->eval_ctx_, topk_limit_))) {
      LOG_WARN("failed to init topk limit", K(ret));
    } else if (OB_UNLIKELY(!ir_ctdef_->need_inv_idx_agg() && ir_ctdef_->need_fwd_idx_agg())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inv idx agg and fwd idx agg are not both needed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      lib::ContextParam mem_param;
      mem_param.set_mem_attr(MTL_ID(), "TextMergeIter", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, mem_param))) {
        LOG_WARN("failed to create text retrieval iterator memory context", K(ret));
      }
    }
    sr_iter_param_.max_batch_size_ = merge_param.max_batch_size_; // may be greater than ir_rtdef_->eval_ctx_->max_batch_size_
    inv_idx_tablet_switched_ = false;
    is_inited_ = true;
    LOG_DEBUG("tr merge iter", K_(function_lookup_mode), K_(topk_mode), K_(daat_mode), K_(taat_mode));
  }
  return ret;
}

int ObDASTRMergeIter::init_das_iter_scan_params()
{
  int ret = OB_SUCCESS;
  const int64_t dim_iter_cnt = taat_mode_ ? 1 : query_tokens_.count();
  // [LT plain MATCH] On the logical-table scoring path doc_agg is intentionally
  // stripped (Provider supplies total_doc_cnt / avg_doc_token_cnt). Skip the
  // total_doc_cnt scan setup when the doc_agg child ctdef is absent — its child
  // index lookup would alias another child and pass a nullptr ctdef downstream.
  if (!ir_ctdef_->need_estimate_total_doc_cnt() && 0 != dim_iter_cnt
      && OB_NOT_NULL(ir_ctdef_->get_doc_agg_ctdef())) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = myself_allocator_.alloc(sizeof(ObTableScanParam)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for total doc cnt scan param", K(ret));
    } else {
      total_doc_cnt_scan_param_ = new (buf) ObTableScanParam();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_das_iter_scan_param(
        ls_id_, total_doc_cnt_tablet_id_,
        ir_ctdef_->get_doc_agg_ctdef(),
        ir_rtdef_->get_doc_agg_rtdef(),
        tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
        *total_doc_cnt_scan_param_))) {
      LOG_WARN("failed to init total doc cnt scan param", K(ret));
    } else {
      static_cast<ObDASScanIter*>(children_[children_cnt_ - 1])->set_scan_param(*total_doc_cnt_scan_param_);
    }
  }

  if (OB_SUCC(ret) && 0 != dim_iter_cnt) {
    if (FALSE_IT(inv_scan_params_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(inv_scan_params_.init(dim_iter_cnt))) {
      LOG_WARN("failed to init inv scan params array", K(ret));
    } else if (OB_FAIL(inv_scan_params_.prepare_allocate(dim_iter_cnt))) {
      LOG_WARN("failed to prepare allocate inv scan params array", K(ret));
    }

    if (OB_FAIL(ret) || !ir_ctdef_->need_inv_idx_agg()) {
    } else if (FALSE_IT(inv_agg_params_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(inv_agg_params_.init(dim_iter_cnt))) {
      LOG_WARN("failed to init inv agg params array", K(ret));
    } else if (OB_FAIL(inv_agg_params_.prepare_allocate(dim_iter_cnt))) {
      LOG_WARN("failed to prepare allocate inv agg params array", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (!ir_ctdef_->need_fwd_idx_agg()) {
    } else if (OB_ISNULL(ir_rtdef_->eval_ctx_) || OB_UNLIKELY(ir_rtdef_->eval_ctx_->max_batch_size_ >= 1)) {
      ret = OB_NOT_IMPLEMENT;
      LOG_ERROR("fwd idx agg with max batch size not implemented", K(ret));
    } else if (FALSE_IT(fwd_scan_params_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(fwd_scan_params_.init(dim_iter_cnt))) {
      LOG_WARN("failed to init fwd scan params array", K(ret));
    } else if (OB_FAIL(fwd_scan_params_.prepare_allocate(dim_iter_cnt))) {
      LOG_WARN("failed to prepare allocate fwd scan params array", K(ret));
    }

    if (OB_SUCC(ret)) {
      void *inv_buf = nullptr;
      if (OB_ISNULL(inv_buf = myself_allocator_.alloc(sizeof(ObTableScanParam) * dim_iter_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for inv scan param", K(ret));
      } else {
        ObTableScanParam *scan_params = static_cast<ObTableScanParam*>(inv_buf);
        for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
          inv_scan_params_[i] = new (&scan_params[i]) ObTableScanParam();
          if (OB_FAIL(init_das_iter_scan_param(
              ls_id_, inv_idx_tablet_id_,
              ir_ctdef_->get_inv_idx_scan_ctdef(), ir_rtdef_->get_inv_idx_scan_rtdef(),
              tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
              *inv_scan_params_[i]))) {
            LOG_WARN("failed to init inv scan param", K(ret));
          } else {
            static_cast<ObDASScanIter*>(children_[i])->set_scan_param(*inv_scan_params_[i]);
          }
        }
      }

      if (OB_SUCC(ret) && ir_ctdef_->need_inv_idx_agg()) {
        void *agg_buf = nullptr;
        if (OB_ISNULL(agg_buf = myself_allocator_.alloc(sizeof(ObTableScanParam) * dim_iter_cnt))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for inv agg param", K(ret));
        } else {
          ObTableScanParam *agg_params = static_cast<ObTableScanParam*>(agg_buf);
          for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
            inv_agg_params_[i] = new (&agg_params[i]) ObTableScanParam();
            if (OB_FAIL(init_das_iter_scan_param(
                ls_id_, inv_idx_tablet_id_,
                ir_ctdef_->get_inv_idx_agg_ctdef(),
                ir_rtdef_->get_inv_idx_agg_rtdef(),
                tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
                *inv_agg_params_[i]))) {
              LOG_WARN("failed to init inv agg param", K(ret));
            } else {
              static_cast<ObDASScanIter*>(children_[i + dim_iter_cnt])->set_scan_param(*inv_agg_params_[i]);
            }
            inv_scan_params_[i]->scan_flag_.scan_order_ = ObQueryFlag::Forward;
          }
        }
      }

      if (OB_SUCC(ret) && ir_ctdef_->need_fwd_idx_agg()) {
        void *fwd_buf = nullptr;
        if (OB_ISNULL(fwd_buf = myself_allocator_.alloc(sizeof(ObTableScanParam) * dim_iter_cnt))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for fwd scan param", K(ret));
        } else {
          ObTableScanParam *fwd_params = static_cast<ObTableScanParam*>(fwd_buf);
          for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
            fwd_scan_params_[i] = new (&fwd_params[i]) ObTableScanParam();
            if (OB_FAIL(init_das_iter_scan_param(
                ls_id_, fwd_idx_tablet_id_,
                ir_ctdef_->get_fwd_idx_agg_ctdef(),
                ir_rtdef_->get_fwd_idx_agg_rtdef(),
                tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
                *fwd_scan_params_[i]))) {
              LOG_WARN("failed to init fwd scan param", K(ret));
            } else {
              static_cast<ObDASScanIter*>(children_[i + dim_iter_cnt * 2])->set_scan_param(*fwd_scan_params_[i]);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::init_block_max_iter_param()
{
  int ret = OB_SUCCESS;
  const int64_t token_cnt = query_tokens_.count();
  block_max_scan_params_.set_allocator(&myself_allocator_);
  if (0 == token_cnt) {
    // do nothing
  } else if (OB_FAIL(block_max_iter_param_.init(*ir_ctdef_, myself_allocator_))) {
    LOG_WARN("failed to init block max iter param", K(ret));
  } else if (OB_FAIL(block_max_scan_params_.init(token_cnt))) {
    LOG_WARN("failed to init block max scan params", K(ret));
  } else if (OB_FAIL(block_max_scan_params_.prepare_allocate(token_cnt))) {
    LOG_WARN("failed to prepare allocate block max scan params", K(ret));
  } else {
    ObTableScanParam *scan_params = nullptr;
    if (OB_ISNULL(scan_params = static_cast<ObTableScanParam *>(myself_allocator_.alloc(sizeof(ObTableScanParam) * token_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for block max scan params", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < token_cnt; ++i) {
      block_max_scan_params_[i] = new (&scan_params[i]) ObTableScanParam();
      if (OB_FAIL(init_das_iter_scan_param(
          ls_id_, inv_idx_tablet_id_,
          ir_ctdef_->get_block_max_scan_ctdef(),
          ir_rtdef_->get_block_max_scan_rtdef(),
          tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
          *block_max_scan_params_[i]))) {
        LOG_WARN("failed to init block max scan param", K(ret));
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::init_bm25_param_est_ctx()
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObDASIRScanCtDef *, 1> ir_scan_ctdefs;
  if (OB_FAIL(ir_scan_ctdefs.push_back(ir_ctdef_))) {
    LOG_WARN("failed to append ir ctdef", K(ret));
  } else if (OB_FAIL(bm25_param_est_ctx_.init(ir_scan_ctdefs, inv_scan_params_,
                                              &myself_allocator_))) {
    LOG_WARN("failed to init bm25 param est ctx", K(ret));
  } else {
    const bool lt_bm25 = ir_ctdef_->need_calc_relevance() && !ir_ctdef_->need_inv_idx_agg();
    const ObDASScanCtDef *inv_ctdef = ir_ctdef_->get_inv_idx_scan_ctdef();
    const bool lt_plain_match_scoring = lt_bm25
        && OB_NOT_NULL(inv_ctdef) && inv_ctdef->is_logical_table();
    if (0 == query_tokens_.count()) {
      // All stopwords / empty tokenization: no logical BM25 provider needed.
    } else if (lt_plain_match_scoring && OB_ISNULL(logical_stats_provider_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT plain MATCH] logical bm25 stats provider required but null "
               "(must be created in init_dim_iter_param)",
               K(ret), KP_(ir_ctdef), KP(inv_ctdef), K_(runtime_ns_lt_loaded),
               K(logical_table_ctx_.namespace_id_), K(logical_table_ctx_.logical_table_id_));
    } else {
      bm25_param_est_ctx_.logical_stats_provider_ = logical_stats_provider_;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (0 == query_tokens_.count()) {
    // skip
  } else if (!ir_ctdef_->need_estimate_total_doc_cnt()) {
    // [LT plain MATCH] Skip total_doc_cnt iter wiring when doc_agg was stripped
    // on the logical-table scoring path — the Provider supplies BM25 params.
    if (OB_ISNULL(ir_ctdef_->get_doc_agg_ctdef()) || OB_ISNULL(total_doc_cnt_scan_param_)) {
      // skip
    } else if (OB_UNLIKELY(!static_cast<sql::ObStoragePushdownFlag>(
        total_doc_cnt_scan_param_->pd_storage_flag_).is_aggregate_pushdown())) {
      ret = OB_NOT_IMPLEMENT;
      LOG_ERROR("aggregate without pushdown not implemented", K(ret));
    } else {
      bm25_param_est_ctx_.total_doc_cnt_iter_ =
          static_cast<sql::ObDASScanIter *>(children_[children_cnt_ - 1]);
    }
  }
  return ret;
}

int ObDASTRMergeIter::init_das_iter_scan_param(const ObLSID &ls_id,
                                               const ObTabletID &tablet_id,
                                               const ObDASScanCtDef *ctdef,
                                               ObDASScanRtDef *rtdef,
                                               transaction::ObTxDesc *tx_desc,
                                               transaction::ObTxReadSnapshot *snapshot,
                                               common::ObArenaAllocator &allocator,
                                               ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(ctdef), KPC(rtdef), K(ls_id), K(tablet_id));
  } else {
    uint64_t tenant_id = MTL_ID();
    scan_param.tenant_id_ = tenant_id;
    scan_param.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
    scan_param.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = false; // scan
    scan_param.is_for_foreign_check_ = false;
    scan_param.timeout_ = rtdef->timeout_ts_;
    scan_param.scan_flag_ = rtdef->scan_flag_;
    scan_param.reserved_cell_count_ = ctdef->access_column_ids_.count();
    scan_param.allocator_ = &rtdef->stmt_allocator_;
    scan_param.scan_allocator_ = &allocator;
    scan_param.sql_mode_ = rtdef->sql_mode_;
    scan_param.frozen_version_ = rtdef->frozen_version_;
    scan_param.force_refresh_lc_ = rtdef->force_refresh_lc_;
    scan_param.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    scan_param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    scan_param.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    scan_param.table_param_ = &(ctdef->table_param_);
    scan_param.op_ = rtdef->p_pd_expr_op_;
    scan_param.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    scan_param.schema_version_ = ctdef->schema_version_;
    scan_param.tenant_schema_version_ = rtdef->tenant_schema_version_;
    scan_param.limit_param_ = rtdef->limit_param_;
    scan_param.need_scn_ = rtdef->need_scn_;
    scan_param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    scan_param.fb_snapshot_ = rtdef->fb_snapshot_;
    scan_param.fb_read_tx_uncommitted_ = rtdef->fb_read_tx_uncommitted_;
    scan_param.ls_id_ = ls_id;
    scan_param.tablet_id_ = tablet_id;
    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      scan_param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    scan_param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_NOT_NULL(tx_desc)) {
      scan_param.tx_id_ = tx_desc->get_tx_id();
    } else {
      scan_param.tx_id_.reset();
    }

    if (OB_NOT_NULL(snapshot)) {
      if (OB_FAIL(scan_param.snapshot_.assign(*snapshot))) {
        LOG_WARN("assign snapshot fail", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null snapshot", K(ret), KP(snapshot));
    }

    if (FAILEDx(scan_param.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("failed to init column ids", K(ret));
    } else if (ctdef->is_logical_table()
               && share::LT_AUX_PRIMARY != ctdef->logical_table_aux_type_
               && common::OB_INVALID_ID != ctdef->physical_data_table_id_) {
      scan_param.index_id_ = ctdef->physical_data_table_id_;
    }
  }
  return ret;
}

int ObDASTRMergeIter::create_dim_iters()
{
  int ret = OB_SUCCESS;
  if (topk_mode_) {
    if (OB_FAIL(init_block_max_iter_param())) {
      LOG_WARN("failed to init block max iter param", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens_.count(); ++i) {
      ObTextRetrievalScanIterParam iter_param;
      ObTextRetrievalBlockMaxIter *dim_iter = nullptr;
      if (OB_FAIL(init_dim_iter_param(iter_param, i))) {
        LOG_WARN("failed to init create tr iter param", K(ret));
      } else if (OB_ISNULL(dim_iter = OB_NEWx(ObTextRetrievalBlockMaxIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for block max iter", K(ret));
      } else if (OB_FAIL(dim_iter->init(iter_param, &block_max_iter_param_, block_max_scan_params_[i]))) {
        LOG_WARN("failed to init text retrieval block max iter", K(ret));
      } else if (OB_FAIL(dim_iters_.push_back(dim_iter))) {
        LOG_WARN("failed to push back dim iter", K(ret));
      }
    }
  } else if (daat_mode_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens_.count(); ++i) {
      ObTextRetrievalScanIterParam iter_param;
      ObTextRetrievalDaaTTokenIter *dim_iter = nullptr;
      if (OB_FAIL(init_dim_iter_param(iter_param, i))) {
        LOG_WARN("failed to init create tr iter param", K(ret));
      } else if (OB_ISNULL(dim_iter = OB_NEWx(ObTextRetrievalDaaTTokenIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for text retrieval daat token iter", K(ret));
      } else if (OB_FAIL(dim_iter->init(iter_param))) {
          LOG_WARN("failed to init text retrieval daat token iter", K(ret));
      } else if (OB_FAIL(dim_iters_.push_back(dim_iter))) {
        LOG_WARN("failed to push back dim iter", K(ret));
      }
    }
  } else if (taat_mode_) {
    ObTextRetrievalScanIterParam iter_param;
    ObTextRetrievalTokenIter *dim_iter = nullptr;
    if (OB_FAIL(init_dim_iter_param(iter_param, 0))) {
      LOG_WARN("failed to init create tr iter param", K(ret));
    } else if (OB_ISNULL(dim_iter = OB_NEWx(ObTextRetrievalTokenIter, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for text retrieval token iter", K(ret));
    } else if (OB_FAIL(dim_iter->init(iter_param))) {
      LOG_WARN("failed to init text retrieval token iter", K(ret));
    } else {
      dim_iter_ = dim_iter;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mode", K(ret), K_(flags));
  }
  return ret;
}

int ObDASTRMergeIter::init_dim_iter_param(ObTextRetrievalScanIterParam &iter_param, const int64_t &idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_rtdef_) || OB_ISNULL(ir_rtdef_->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), KP_(ir_ctdef), KP_(ir_rtdef));
  } else {
    const int64_t dim_iter_cnt = taat_mode_ ? 1 : query_tokens_.count();
    iter_param.inv_idx_scan_param_ = inv_scan_params_[idx];
    iter_param.inv_idx_scan_iter_ = static_cast<ObDASScanIter*>(children_[idx]);
    if (ir_ctdef_->need_inv_idx_agg()) {
      iter_param.inv_idx_agg_param_ = inv_agg_params_[idx];
      iter_param.inv_idx_agg_iter_ = static_cast<ObDASScanIter*>(children_[idx + dim_iter_cnt]);
      if (OB_UNLIKELY(1 != ir_ctdef_->get_inv_idx_agg_ctdef()
          ->pd_expr_spec_.pd_storage_aggregate_output_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected inv idx agg expr count", K(ret));
      } else {
        iter_param.inv_idx_agg_expr_ = ir_ctdef_->get_inv_idx_agg_ctdef()
            ->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
      }
    }
    if (OB_SUCC(ret) && ir_ctdef_->need_fwd_idx_agg()) {
      iter_param.fwd_idx_scan_param_ = fwd_scan_params_[idx];
      iter_param.fwd_idx_agg_iter_ = static_cast<ObDASScanIter*>(children_[idx + dim_iter_cnt * 2]);
      if (OB_UNLIKELY(1 != ir_ctdef_->get_fwd_idx_agg_ctdef()
          ->pd_expr_spec_.pd_storage_aggregate_output_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected fwd idx agg expr count", K(ret));
      } else {
        iter_param.fwd_idx_agg_expr_ = ir_ctdef_->get_fwd_idx_agg_ctdef()
            ->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
      }
    }
    iter_param.allocator_ = &myself_allocator_;
    iter_param.eval_ctx_ = ir_rtdef_->eval_ctx_;
    iter_param.relevance_expr_ = ir_ctdef_->relevance_expr_;
    iter_param.inv_scan_doc_length_col_ = ir_ctdef_->inv_scan_doc_length_col_;
    iter_param.inv_scan_domain_id_col_ = ir_ctdef_->inv_scan_domain_id_col_;
    iter_param.reuse_inv_idx_agg_res_ = !taat_mode_ && ir_ctdef_->need_inv_idx_agg();
    if (OB_SUCC(ret) && OB_FAIL(init_logical_fts_dim_iter_param_(iter_param, idx))) {
      LOG_WARN("failed to init logical fts dim iter param", K(ret));
    }
  }
  return ret;
}

int ObDASTRMergeIter::init_logical_fts_dim_iter_param_(
    ObTextRetrievalScanIterParam &iter_param,
    const int64_t idx)
{
  int ret = OB_SUCCESS;
  // When the inverted scan ctdef has been KV-rewritten for a logical table,
  // propagate logical_index_seq_ for advance_to KV range rewrite. ns/lt for BM25
  // stats come from logical_table_ctx_ via logical_stats_provider_;
  // logical_index_seq_ is a static ctdef field (per aux) and stays on ctdef.
  const ObDASScanCtDef *inv_ctdef = ir_ctdef_->get_inv_idx_scan_ctdef();
  // FTS aux ctdef is KV-rewritten via rewrite_fts_aux_ctdef_for_logical_table_, which
  // only sets is_logical_table() + logical_index_seq_; lt_ctx_.analyzed_ is for main-table
  // PK predicate baking (analyze_logical_table_filters_) and stays false on aux scans.
  // Runtime ns/lt comes from the inverted scan rtdef's lt_rt_, which ObDASScanOp::
  // init_scan_param fills by evaluating the ns/lt exprs baked onto lt_ctx_ by
  // ObHybridSearchCgService::inject_logical_table_ns_lt_into_fts_inv_scan_ctdef.
  if (OB_ISNULL(inv_ctdef) || !inv_ctdef->is_logical_table()) {
    // not a logical-table FTS aux scan
  } else if (OB_FAIL(ensure_logical_ns_lt_loaded_())) {
    LOG_WARN("[LT plain MATCH] failed to load runtime ns/lt for FTS aux", K(ret));
  } else if (!runtime_ns_lt_loaded_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT plain MATCH] runtime ns/lt unavailable for FTS aux iter "
             "(lt_rt_ not resolved from ctdef ns/lt exprs)", K(ret));
  } else {
    iter_param.is_logical_fts_    = true;
    iter_param.logical_index_seq_ = inv_ctdef->logical_index_seq_;
    if (OB_NOT_NULL(ir_ctdef_->search_text_)) {
      iter_param.query_token_meta_ = ir_ctdef_->search_text_->obj_meta_;
    }
    const int64_t token_idx = taat_mode_ ? 0 : idx;
    if (token_idx >= 0 && token_idx < query_tokens_.count()) {
      iter_param.query_token_ = query_tokens_.at(token_idx);
    }
    // When scoring is preserved but inv_idx_agg was stripped
    // (need_calc_relevance && !need_inv_idx_agg), acquire the filtered BM25
    // stats provider once for this merge iter and feed every per-token iter.
    if (ir_ctdef_->need_calc_relevance() && !ir_ctdef_->need_inv_idx_agg()) {
      if (OB_FAIL(ensure_logical_stats_provider_(inv_ctdef))) {
        LOG_WARN("failed to ensure logical bm25 stats provider", K(ret));
      } else {
        iter_param.logical_stats_provider_ = logical_stats_provider_;
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::set_related_tablet_ids(
    const ObLSID &ls_id,
    const ObDASFTSTabletID &related_tablet_ids)
{
  int ret = OB_SUCCESS;
  const ObTabletID old_logic_main_anchor = total_doc_cnt_tablet_id_;
  ls_id_ = ls_id;
  // On lt_filtered_bm25, domain_id slot holds stats_primary logic-main anchor (not doc_agg).
  total_doc_cnt_tablet_id_ = related_tablet_ids.domain_id_idx_tablet_id_;
  const bool lt_filtered_bm25 = OB_NOT_NULL(ir_ctdef_)
      && ir_ctdef_->has_stats_primary_scan_
      && ir_ctdef_->need_calc_relevance()
      && !ir_ctdef_->need_inv_idx_agg();
  if (lt_filtered_bm25) {
    if (old_logic_main_anchor.is_valid()
        && old_logic_main_anchor != total_doc_cnt_tablet_id_) {
      inv_idx_tablet_switched_ = true;
    }
    if (OB_FAIL(patch_logical_kv_inv_tablet_id_())) {
      LOG_WARN("[LT plain MATCH] failed to patch kv inv tablet id", K(ret),
               K(total_doc_cnt_tablet_id_));
    }
  } else {
    if (inv_idx_tablet_id_.is_valid()
        && inv_idx_tablet_id_ != related_tablet_ids.inv_idx_tablet_id_) {
      inv_idx_tablet_switched_ = true;
    }
    inv_idx_tablet_id_ = related_tablet_ids.inv_idx_tablet_id_;
  }
  fwd_idx_tablet_id_ = related_tablet_ids.fwd_idx_tablet_id_;
  return ret;
}

int ObDASTRMergeIter::patch_logical_kv_inv_tablet_id_()
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *inv_ctdef = OB_NOT_NULL(ir_ctdef_) ? ir_ctdef_->get_inv_idx_scan_ctdef() : nullptr;
  const ObDASScanCtDef *primary_ctdef = OB_NOT_NULL(ir_ctdef_) ? ir_ctdef_->get_stats_primary_scan_ctdef() : nullptr;
  if (OB_ISNULL(ir_ctdef_) || OB_ISNULL(inv_ctdef) || OB_ISNULL(primary_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT plain MATCH] unexpected null ctdef for kv inv tablet patch",
             K(ret), KP(ir_ctdef_), KP(inv_ctdef), KP(primary_ctdef));
  } else if (!inv_ctdef->is_logical_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT plain MATCH] expected logical inv ctdef for kv inv tablet patch",
             K(ret), KP(inv_ctdef));
  } else if (!total_doc_cnt_tablet_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT plain MATCH] logic-main anchor tablet invalid for kv inv patch",
             K(ret), K(total_doc_cnt_tablet_id_));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const uint64_t logical_ref_table_id = primary_ctdef->ref_table_id_;
    const uint64_t kv_table_id = primary_ctdef->physical_data_table_id_;
    ObTabletID kv_tablet_id;
    ObObjectID kv_object_id = OB_INVALID_OBJECT_ID;
    if (OB_FAIL(ObLogicalTableHelper::resolve_kv_tablet_via_logical_anchor(
            tenant_id, logical_ref_table_id, kv_table_id,
            total_doc_cnt_tablet_id_, kv_tablet_id, kv_object_id))) {
      LOG_WARN("[LT plain MATCH] resolve kv tablet for inv scan failed", K(ret),
               K(tenant_id), K(logical_ref_table_id), K(kv_table_id),
               K(total_doc_cnt_tablet_id_));
    } else {
      inv_idx_tablet_id_ = kv_tablet_id;
      LOG_TRACE("[LT plain MATCH] patched inv_idx_tablet_id_ to kv physical tablet",
               K(total_doc_cnt_tablet_id_), K(inv_idx_tablet_id_), K(kv_object_id));
    }
  }
  return ret;
}

int ObDASTRMergeIter::ensure_logical_ns_lt_loaded_()
{
  // Read propagated ns/lt from inv scan rtdef lt_rt and cache on logical_table_ctx_.
  // Idempotent within one execution.
  int ret = OB_SUCCESS;
  if (runtime_ns_lt_loaded_) {
    // already loaded for this execution
  } else if (OB_ISNULL(ir_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT plain MATCH] unexpected null ir_rtdef", K(ret), KP(ir_rtdef_));
  } else {
    ObDASScanRtDef *inv_rtdef = ir_rtdef_->get_inv_idx_scan_rtdef();
    if (OB_ISNULL(inv_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT plain MATCH] inv scan rtdef is null", K(ret));
    } else {
      const ObLogicalTableRtDef &lt_rt = inv_rtdef->lt_rt_;
      if (lt_rt.has_resolved_ns_lt()) {
        logical_table_ctx_.namespace_id_ = lt_rt.namespace_id_;
        logical_table_ctx_.logical_table_id_ = lt_rt.logical_table_id_;
        logical_table_ctx_.is_enabled_ = true;
        runtime_ns_lt_loaded_ = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LT plain MATCH] lt_rt missing ns/lt for tr merge", K(ret), K(lt_rt));
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::ensure_logical_stats_provider_(const ObDASScanCtDef *inv_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inv_ctdef) || OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), KP(inv_ctdef), KP_(ir_ctdef), KP_(ir_rtdef));
  } else if (ir_ctdef_->has_stats_primary_scan_
             && !total_doc_cnt_tablet_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT plain MATCH] logic-main anchor missing for stats_primary",
             K(ret), K(total_doc_cnt_tablet_id_), K(inv_idx_tablet_id_));
  } else if (!runtime_ns_lt_loaded_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT plain MATCH] runtime ns/lt not loaded before stats provider", K(ret));
  } else if (!inv_idx_tablet_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT plain MATCH] kv inv tablet id invalid before stats provider",
             K(ret), K(inv_idx_tablet_id_), K(total_doc_cnt_tablet_id_));
  } else if (OB_NOT_NULL(logical_stats_provider_)) {
    // already cached for this execution; ns/lt are immutable within a SQL
  }
  if (OB_SUCC(ret) && OB_ISNULL(logical_stats_provider_)) {
    const ObDASScanCtDef *primary_ctdef = ir_ctdef_->get_stats_primary_scan_ctdef();
    ObDASScanRtDef *primary_rtdef = ir_rtdef_->get_stats_primary_scan_rtdef();
    if (OB_ISNULL(primary_ctdef) || OB_ISNULL(primary_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT plain MATCH] missing stats_primary ctdef/rtdef for logical bm25",
               K(ret), KP(primary_ctdef), KP(primary_rtdef));
    } else {
      // Provider / inv FTS aux scans use KV physical tablet (patched in set_related_tablet_ids).
      logical_table_ctx_.is_enabled_ = true;
      logical_table_ctx_.physical_data_table_id_ = inv_ctdef->physical_data_table_id_;
      logical_table_ctx_.logical_table_aux_type_ = inv_ctdef->logical_table_aux_type_;
      logical_table_ctx_.physical_tablet_id_ = inv_idx_tablet_id_;

      void *buf = myself_allocator_.alloc(sizeof(ObLogicalFilteredBM25StatsProvider));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc logical bm25 stats provider", K(ret));
      } else {
        ObLogicalFilteredBM25StatsProvider *provider =
            new (buf) ObLogicalFilteredBM25StatsProvider();
        ObLogicalFilteredBM25StatsKey key;
        key.namespace_id_ = logical_table_ctx_.namespace_id_;
        key.logical_table_id_ = logical_table_ctx_.logical_table_id_;
        key.logical_index_seq_ = inv_ctdef->logical_index_seq_;
        key.physical_data_table_id_ = inv_ctdef->physical_data_table_id_;
        key.physical_tablet_id_ = inv_idx_tablet_id_;
        key.ir_ctdef_ = ir_ctdef_;
        key.filter_exprs_ = &ir_ctdef_->logical_stats_filter_exprs_;
        const ObDASBaseCtDef *primary_base = static_cast<const ObDASBaseCtDef *>(primary_ctdef);
        ObDASBaseRtDef *primary_base_rt = static_cast<ObDASBaseRtDef *>(primary_rtdef);
        const ObDASBaseCtDef *inv_base = static_cast<const ObDASBaseCtDef *>(inv_ctdef);
        ObDASBaseRtDef *inv_base_rt = static_cast<ObDASBaseRtDef *>(
            ir_rtdef_->get_inv_idx_scan_rtdef());
        if (OB_FAIL(provider->init(key, primary_base, primary_base_rt,
                                    inv_base, inv_base_rt,
                                    &ir_ctdef_->logical_stats_filter_exprs_,
                                    false /*use_scalar_ctdef*/, this))) {
          LOG_WARN("failed to init logical bm25 stats provider", K(ret), K(key));
          provider->~ObLogicalFilteredBM25StatsProvider();
        } else {
          logical_stats_provider_ = provider;
        }
      }
    }
  }
  return ret;
}

void ObDASTRMergeIter::destroy_logical_stats_provider_()
{
  if (OB_NOT_NULL(logical_stats_provider_)) {
    logical_stats_provider_->~ObLogicalFilteredBM25StatsProvider();
    logical_stats_provider_ = nullptr;
  }
  // Reset only provider-specific lazy fields; keep ns/lt because they are
  // session-derived runtime values that remain valid within this execution
  // (a tablet switch during rescan does not change @namespace_id / @ltable_id).
  logical_table_ctx_.is_enabled_ = false;
  logical_table_ctx_.physical_data_table_id_ = common::OB_INVALID_ID;
  logical_table_ctx_.logical_table_aux_type_ = share::LT_AUX_MAX_TYPE;
  logical_table_ctx_.physical_tablet_id_.reset();
}

ObEvalCtx *ObDASTRMergeIter::get_eval_ctx()
{
  return OB_NOT_NULL(ir_rtdef_) ? ir_rtdef_->eval_ctx_ : nullptr;
}

int ObDASTRMergeIter::get_related_tablet_id(const ObDASBaseCtDef *ctdef,
                                            common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ctdef", K(ret));
  } else if (ctdef->is_logical_table()
             && share::LT_AUX_PRIMARY == ctdef->logical_table_aux_type_) {
    // LT_AUX_PRIMARY: logic-main partition tablet before KV rewrite (domain slot).
    if (!total_doc_cnt_tablet_id_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT plain MATCH] logic-main anchor tablet invalid for LT_AUX_PRIMARY",
               K(ret), KPC(ctdef),
               "ref_table_id", static_cast<const ObDASScanCtDef *>(ctdef)->ref_table_id_);
    } else {
      tablet_id = total_doc_cnt_tablet_id_;
    }
  } else if (!inv_idx_tablet_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT plain MATCH] inv idx tablet id invalid", K(ret), K(inv_idx_tablet_id_));
  } else {
    tablet_id = inv_idx_tablet_id_;
  }
  return ret;
}

int ObDASTRMergeIter::init_scan_param(common::ObTabletID &tablet_id,
                                      const ObDASBaseCtDef *ctdef,
                                      ObDASBaseRtDef *rtdef,
                                      storage::ObTableScanParam &scan_param)
{
  // Plain MATCH only ever passes ObDASScanCtDef-typed children — delegate to
  // the existing static helper that already understands that subtype.
  return init_das_iter_scan_param(ls_id_, tablet_id,
                                  static_cast<const ObDASScanCtDef *>(ctdef),
                                  static_cast<ObDASScanRtDef *>(rtdef),
                                  tx_desc_, snapshot_,
                                  mem_context_->get_arena_allocator(),
                                  scan_param);
}

int ObDASTRMergeIter::create_sparse_retrieval_iter()
{
  int ret = OB_SUCCESS;
  sr_iter_param_.limit_param_ = &ir_rtdef_->get_inv_idx_scan_rtdef()->limit_param_;
  sr_iter_param_.eval_ctx_ = ir_rtdef_->eval_ctx_;
  sr_iter_param_.id_proj_expr_ = ir_ctdef_->inv_scan_domain_id_col_;
  sr_iter_param_.relevance_expr_ = ir_ctdef_->relevance_expr_;
  sr_iter_param_.relevance_proj_expr_ = ir_ctdef_->relevance_proj_col_;
  sr_iter_param_.filter_expr_ = ir_ctdef_->match_filter_;
  sr_iter_param_.topk_limit_ = topk_limit_;
  if (topk_mode_) {
    ObTextDaaTParam iter_param;
    ObTextBMWIter *bmw_iter = nullptr;
    if (OB_FAIL(init_daat_iter_param(iter_param))) {
      LOG_WARN("failed to init sr iter param", K(ret));
    } else if (OB_ISNULL(bmw_iter = OB_NEWx(ObTextBMWIter, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for text retrieval bm25 iter", K(ret));
    } else if (OB_FAIL(bmw_iter->init(iter_param))) {
      LOG_WARN("failed to init text retrieval bm25 iter", K(ret));
    } else {
      sparse_retrieval_iter_ = bmw_iter;
    }
  } else if (daat_mode_) {
    ObTextDaaTParam iter_param;
    ObTextDaaTIter *daat_iter = nullptr;
    if (OB_FAIL(init_daat_iter_param(iter_param))) {
      LOG_WARN("failed to init sr iter param", K(ret));
    } else if (OB_ISNULL(daat_iter = OB_NEWx(ObTextDaaTIter, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for text retrieval daat iter", K(ret));
    } else if (OB_FAIL(daat_iter->init(iter_param))) {
      LOG_WARN("failed to init text retrieval daat iter", K(ret));
    } else {
      sparse_retrieval_iter_ = daat_iter;
    }
  } else if (taat_mode_) {
    ObTextTaaTParam iter_param;
    ObTextTaaTIter *taat_iter = nullptr;
    if (OB_FAIL(init_taat_iter_param(iter_param))) {
      LOG_WARN("failed to init sr iter param", K(ret));
    } else if (OB_ISNULL(taat_iter = OB_NEWx(ObTextTaaTIter, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for text retrieval taat iter", K(ret));
    } else if (OB_FAIL(taat_iter->init(iter_param))) {
      LOG_WARN("failed to init text retrieval taat iter", K(ret));
    } else {
      sparse_retrieval_iter_ = taat_iter;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mode", K(ret), K_(flags));
  }
  if (OB_SUCC(ret) && function_lookup_mode_) {
    ObSRLookupIter *lookup_iter = nullptr;
    if (OB_UNLIKELY(topk_mode_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected both topk mode and function lookup mode", K(ret));
    } else if (daat_mode_) {
      if (OB_ISNULL(lookup_iter = OB_NEWx(ObSRSortedLookupIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for sorted lookup iter", K(ret));
      }
    } else if (taat_mode_) {
      if (OB_ISNULL(lookup_iter = OB_NEWx(ObSRHashLookupIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for hash lookup iter", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mode", K(ret), K_(flags));
    }
    if (FAILEDx(lookup_iter->init(sr_iter_param_, *sparse_retrieval_iter_,
                                  myself_allocator_, sr_iter_param_.max_batch_size_))) {
      LOG_WARN("failed to init lookup iter", K(ret));
    } else {
      sparse_retrieval_iter_ = lookup_iter;
    }
  }
  return ret;
}

int ObDASTRMergeIter::init_daat_iter_param(ObTextDaaTParam &iter_param)
{
  int ret = OB_SUCCESS;
  iter_param.dim_iters_ = &dim_iters_;
  iter_param.base_param_ = &sr_iter_param_;
  iter_param.allocator_ = &myself_allocator_;
  iter_param.bm25_param_est_ctx_ = &bm25_param_est_ctx_;
  iter_param.mode_flag_ = ir_ctdef_->mode_flag_;
  iter_param.function_lookup_mode_ = function_lookup_mode_;
  if (OB_FAIL(init_bm25_param_est_ctx())) {
    LOG_WARN("failed to init bm25 param est ctx", K(ret));
  } else {
    if (BOOLEAN_MODE == ir_ctdef_->mode_flag_) {
      ObSRDaaTBooleanRelevanceCollector *boolean_relevance_collector = nullptr;
      if (OB_ISNULL(boolean_relevance_collector = OB_NEWx(ObSRDaaTBooleanRelevanceCollector, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for boolean relevance collector", K(ret));
      } else if (OB_FAIL(boolean_relevance_collector->init(&myself_allocator_, dim_iters_.count(), boolean_compute_node_))) {
        LOG_WARN("failed to init boolean relevance collector", K(ret));
      } else {
        iter_param.relevance_collector_ = boolean_relevance_collector;
      }
    } else {
      ObSRDaaTInnerProductRelevanceCollector *inner_product_relevance_collector = nullptr;
      if (OB_ISNULL(inner_product_relevance_collector = OB_NEWx(ObSRDaaTInnerProductRelevanceCollector, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for inner product relevance collector", K(ret));
      } else if (OB_FAIL(inner_product_relevance_collector->init())) {
        LOG_WARN("failed to init boolean relevance collector", K(ret));
      } else {
        iter_param.relevance_collector_ = inner_product_relevance_collector;
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::init_taat_iter_param(ObTextTaaTParam &iter_param)
{
  int ret = OB_SUCCESS;
  iter_param.query_tokens_ = &query_tokens_;
  iter_param.base_param_ = &sr_iter_param_;
  iter_param.allocator_ = &myself_allocator_;
  iter_param.bm25_param_est_ctx_ = &bm25_param_est_ctx_;
  iter_param.mode_flag_ = ir_ctdef_->mode_flag_;
  iter_param.function_lookup_mode_ = function_lookup_mode_;
  if (OB_ISNULL(iter_param.dim_iter_ = dim_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null dim iter", K(ret));
  } else if (OB_FAIL(init_bm25_param_est_ctx())) {
    LOG_WARN("failed to init bm25 param est ctx", K(ret));
  }
  return ret;
}

int ObDASTRMergeIter::set_children_iter_rangekey()
{
  int ret = OB_SUCCESS;
  ObNewRange fwd_idx_agg_range;
  check_rangekey_inited_ = true;
  if (OB_UNLIKELY(function_lookup_mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected function lookup mode", K(ret), K_(flags));
  } else if (OB_UNLIKELY(0 == query_tokens_.count())) {
    // do nothing
  } else if (OB_UNLIKELY(inv_scan_params_.empty()
      || (ir_ctdef_->need_inv_idx_agg() && inv_agg_params_.empty()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected uninited scan params", K(ret));
  } else if (ir_ctdef_->need_fwd_idx_agg()
      && OB_FAIL(gen_fwd_idx_scan_feak_range(ir_ctdef_, fwd_idx_agg_range))) {
    LOG_WARN("failed to generate fwd idx scan range", K(ret));
  } else {
    const ExprFixedArray *exprs = &(ir_ctdef_->get_inv_idx_scan_ctdef()->pd_expr_spec_.access_exprs_);
    int64 group_id = 0;
    for (int64_t i = 0; i < exprs->count(); ++i) {
      if (T_PSEUDO_GROUP_ID == exprs->at(i)->type_) {
        group_id = exprs->at(i)->locate_expr_datum(*eval_ctx_).get_int();
      }
    }
    int64_t group_idx = ObNewRange::get_group_idx(group_id);
    ObNewRange inv_idx_scan_range;
    const int64_t dim_iter_cnt = taat_mode_ ? 1 : query_tokens_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
      if (OB_FAIL(gen_inv_idx_scan_default_range(ir_ctdef_,
                                                 query_tokens_[i],
                                                 mem_context_->get_arena_allocator(),
                                                 inv_idx_scan_range))) {
        LOG_WARN("failed to generate inverted index scan range", K(ret), K(query_tokens_[i]));
      } else if (ir_ctdef_->need_inv_idx_agg()
          && OB_FAIL(inv_agg_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
        LOG_WARN("failed to push back lookup range", K(ret));
      } else if (FALSE_IT(inv_idx_scan_range.group_idx_ = group_idx)) {
      } else if (OB_FAIL(inv_scan_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
        LOG_WARN("failed to push back lookup range", K(ret));
      } else if (ir_ctdef_->need_fwd_idx_agg()
          && OB_FAIL(fwd_scan_params_[i]->key_ranges_.push_back(fwd_idx_agg_range))) {
        LOG_WARN("failed to push back lookup range", K(ret));
      } else if (topk_mode_ && ir_ctdef_->need_block_max_scan()
          && OB_FAIL(block_max_scan_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
        LOG_WARN("failed to push back lookup range", K(ret));
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::set_children_iter_rangekey(const common::ObIArray<std::pair<ObDocIdExt, int>> &virtual_rangekeys, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  check_rangekey_inited_ = true;
  if (OB_UNLIKELY(!function_lookup_mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-function lookup mode", K(ret), K_(flags));
  } else if (nullptr == sparse_retrieval_iter_) {
    if (OB_FAIL(init_das_iter_scan_params())) {
      LOG_WARN("failed to init das iter scan params", K(ret));
    } else if (OB_FAIL(create_dim_iters())) {
      LOG_WARN("failed to create dim iters", K(ret));
    } else if (OB_FAIL(create_sparse_retrieval_iter())) {
      LOG_WARN("failed to create sparse retrieval iter", K(ret));
    }
  }
  if (OB_FAIL(ret) || 0 == query_tokens_.count()) {
  } else if (OB_UNLIKELY(inv_scan_params_.empty()
      || (ir_ctdef_->need_inv_idx_agg() && inv_agg_params_.empty())
      || ir_ctdef_->need_fwd_idx_agg())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected uninited scan params", K(ret));
  } else {
    const ExprFixedArray *exprs = &(ir_ctdef_->get_inv_idx_scan_ctdef()->pd_expr_spec_.access_exprs_);
    int64 group_id = 0;
    for (int64_t i = 0; i < exprs->count(); ++i) {
      if (T_PSEUDO_GROUP_ID == exprs->at(i)->type_) {
        group_id = exprs->at(i)->locate_expr_datum(*eval_ctx_).get_int();
      }
    }
    int64_t group_idx = ObNewRange::get_group_idx(group_id);
    ObNewRange inv_idx_scan_range;
    const int64_t dim_iter_cnt = taat_mode_ ? 1 : query_tokens_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (OB_FAIL(gen_inv_idx_scan_one_range(ir_ctdef_,
                                               query_tokens_[i],
                                               virtual_rangekeys.at(j).first,
                                               mem_context_->get_arena_allocator(),
                                               inv_idx_scan_range))) {
          LOG_WARN("failed to generate inverted index scan range", K(ret),
                   K(query_tokens_[i]), K(virtual_rangekeys.at(j).first));
        } else if (FALSE_IT(inv_idx_scan_range.group_idx_ = group_idx)) {
        } else if (OB_FAIL(inv_scan_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
          LOG_WARN("failed to push back lookup range", K(ret));
        }
      }

      if (!ir_ctdef_->need_inv_idx_agg()) {
      } else if (OB_FAIL(gen_inv_idx_scan_default_range(ir_ctdef_,
                                                        query_tokens_[i],
                                                        mem_context_->get_arena_allocator(),
                                                        inv_idx_scan_range))) {
        LOG_WARN("failed to generate inverted index scan range", K(ret), K(query_tokens_[i]));
      } else if (OB_FAIL(inv_agg_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
        LOG_WARN("failed to push back lookup range", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sparse retrieval iter", K(ret));
  } else if (OB_FAIL(static_cast<ObSRLookupIter *>(sparse_retrieval_iter_)
      ->set_hints(virtual_rangekeys, batch_size))) {
    LOG_WARN("failed to set hints", K(ret), K(batch_size));
  }
  return ret;
}

int ObDASTRMergeIter::adjust_topk_limit(const int64_t limit)
{
  int ret = OB_SUCCESS;
  topk_limit_ = limit;
  
  if (!topk_mode_) {
    // do nothing
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sparse retrieval iter", K(ret));
  } else {
    storage::ObTextBMWIter *bmw_iter = static_cast<storage::ObTextBMWIter *>(sparse_retrieval_iter_);
    if (OB_FAIL(bmw_iter->adjust_topk_limit(limit))) {
      LOG_WARN("failed to adjust topk limit for bmw iter", K(ret), K(limit));
    }
  }
  return ret;
}

int ObDASTRMergeIter::gen_inv_idx_scan_default_range(const ObDASIRScanCtDef *ir_ctdef,
                                                     const ObString &query_token,
                                                     common::ObArenaAllocator &allocator,
                                                     ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObObj *obj_ptr = nullptr;
  constexpr int64_t obj_cnt = INV_IDX_ROWKEY_COL_CNT * 2;
  ObObj tmp_obj;
  tmp_obj.set_string(ObVarcharType, query_token);
  // We need to ensure collation type / level between query text and token column is compatible
  tmp_obj.set_meta_type(ir_ctdef->search_text_->obj_meta_);

  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * obj_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for rowkey obj", K(ret));
  } else if (OB_ISNULL(obj_ptr = new (buf) ObObj[obj_cnt])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(ob_write_obj(allocator, tmp_obj, obj_ptr[0]))) {
    LOG_WARN("failed to write obj", K(ret));
  } else if (OB_FAIL(ob_write_obj(allocator, tmp_obj, obj_ptr[2]))) {
    LOG_WARN("failed to write obj", K(ret));
  } else {
    obj_ptr[1].set_min_value();
    obj_ptr[3].set_max_value();
    ObRowkey start_key(obj_ptr, INV_IDX_ROWKEY_COL_CNT);
    ObRowkey end_key(&obj_ptr[2], INV_IDX_ROWKEY_COL_CNT);
    common::ObTableID inv_table_id = ir_ctdef->get_inv_idx_scan_ctdef()->ref_table_id_;
    scan_range.table_id_ = inv_table_id;
    scan_range.start_key_.assign(obj_ptr, INV_IDX_ROWKEY_COL_CNT);
    scan_range.end_key_.assign(&obj_ptr[2], INV_IDX_ROWKEY_COL_CNT);
    scan_range.border_flag_.set_inclusive_start();
    scan_range.border_flag_.set_inclusive_end();
  }
  return ret;
}

int ObDASTRMergeIter::gen_inv_idx_scan_one_range(const ObDASIRScanCtDef *ir_ctdef,
                                                 const ObString &query_token,
                                                 const ObDocIdExt &doc_id,
                                                 common::ObArenaAllocator &allocator,
                                                 ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObObj *obj_ptr = nullptr;
  constexpr int64_t obj_cnt = INV_IDX_ROWKEY_COL_CNT;
  ObObj tmp_obj;
  tmp_obj.set_string(ObVarcharType, query_token);
  // We need to ensure collation type / level between query text and token column is compatible
  tmp_obj.set_meta_type(ir_ctdef->search_text_->obj_meta_);

  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * obj_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for rowkey obj", K(ret));
  } else if (OB_ISNULL(obj_ptr = new (buf) ObObj[obj_cnt])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(ob_write_obj(allocator, tmp_obj, obj_ptr[0]))) {
    LOG_WARN("failed to write obj", K(ret));
  } else if (OB_FAIL(doc_id.get_datum().to_obj(obj_ptr[1], ir_ctdef->inv_scan_domain_id_col_->obj_meta_))) {
    LOG_WARN("failed to set obj", K(ret));
  } else {
    ObRowkey row_key(obj_ptr, obj_cnt);
    common::ObTableID inv_table_id = ir_ctdef->get_inv_idx_scan_ctdef()->ref_table_id_;
    if (OB_FAIL(scan_range.build_range(inv_table_id, row_key))) {
      LOG_WARN("failed to build lookup range", K(ret), K(inv_table_id), K(row_key));
    }
  }
  return ret;
}

int ObDASTRMergeIter::gen_fwd_idx_scan_feak_range(const ObDASIRScanCtDef *ir_ctdef,
                                                  ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  scan_range.table_id_ = ir_ctdef->get_fwd_idx_agg_ctdef()->ref_table_id_;
  return ret;
}

int ObDASTRMergeIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    if (function_lookup_mode_) {
      // skip the following, which should have been done
    } else if (OB_FAIL(init_das_iter_scan_params())) {
      LOG_WARN("failed to init das iter scan params", K(ret));
    } else if (OB_FAIL(create_dim_iters())) {
      LOG_WARN("failed to create dim iters", K(ret));
    } else if (OB_FAIL(create_sparse_retrieval_iter())) {
      LOG_WARN("failed to create sparse retrieval iter", K(ret));
    } else if (OB_FAIL(set_children_iter_rangekey())) {
      LOG_WARN("failed to set children iter rangekey", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!check_rangekey_inited_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rangekey is not inited", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      if (OB_FAIL(children_[i]->do_table_scan())) {
        LOG_WARN("failed to do table scan", K(ret));
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::reuse_das_iter_scan_param(const ObLSID &ls_id,
                                                const ObTabletID &tablet_id,
                                                ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  const ObTabletID &old_tablet_id = scan_param.tablet_id_;
  scan_param.need_switch_param_ = scan_param.need_switch_param_
      || (old_tablet_id.is_valid() && old_tablet_id != tablet_id);
  if (!scan_param.key_ranges_.empty()) {
    scan_param.key_ranges_.reuse();
  }
  scan_param.tablet_id_ = tablet_id;
  scan_param.ls_id_ = ls_id;
  return ret;
}

int ObDASTRMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sparse retrieval iter is null", K(ret));
  } else if (query_tokens_.count() > 0) {
    if (OB_NOT_NULL(total_doc_cnt_scan_param_)) {
      if (OB_FAIL(reuse_das_iter_scan_param(ls_id_, total_doc_cnt_tablet_id_, *total_doc_cnt_scan_param_))) {
        LOG_WARN("failed to reuse total doc cnt scan param", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inv_scan_params_.count(); ++i) {
      if (OB_FAIL(reuse_das_iter_scan_param(ls_id_, inv_idx_tablet_id_, *inv_scan_params_[i]))) {
        LOG_WARN("failed to reuse inv scan param", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inv_agg_params_.count(); ++i) {
      if (OB_FAIL(reuse_das_iter_scan_param(ls_id_, inv_idx_tablet_id_, *inv_agg_params_[i]))) {
        LOG_WARN("failed to reuse inv agg param", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < block_max_scan_params_.count(); ++i) {
      if (OB_FAIL(reuse_das_iter_scan_param(ls_id_, inv_idx_tablet_id_, *block_max_scan_params_[i]))) {
        LOG_WARN("failed to reuse block max scan param", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < fwd_scan_params_.count(); ++i) {
      if (OB_FAIL(reuse_das_iter_scan_param(ls_id_, fwd_idx_tablet_id_, *fwd_scan_params_[i]))) {
        LOG_WARN("failed to reuse fwd scan param", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      if (OB_FAIL(children_[i]->reuse())) {
        LOG_WARN("failed to reuse child", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    sparse_retrieval_iter_->reuse(inv_idx_tablet_switched_);
    // When the physical tablet changes between rescans the cached filtered
    // doc set (Provider state) no longer matches the new tablet — drop it so
    // the next get_or_create rebuilds against the new tablet.
    if (inv_idx_tablet_switched_) {
      destroy_logical_stats_provider_();
    }
    inv_idx_tablet_switched_ = false;
  }

  if (OB_NOT_NULL(mem_context_)) {
    mem_context_->reset_remain_one_page();
  }
  check_rangekey_inited_ = false;
  return ret;
}

int ObDASTRMergeIter::rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sparse retrieval iter is null", K(ret));
  } else if (!function_lookup_mode_ && OB_FAIL(set_children_iter_rangekey())) {
    LOG_WARN("failed to set children iter rangekey", K(ret));
  } else if (OB_UNLIKELY(!check_rangekey_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rangekey is not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      // TODO: update scan param
      if (OB_FAIL(children_[i]->rescan())) {
        LOG_WARN("failed to do table scan", K(ret));
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(total_doc_cnt_scan_param_)) {
    total_doc_cnt_scan_param_->destroy_schema_guard();
    total_doc_cnt_scan_param_->snapshot_.reset();
    total_doc_cnt_scan_param_->destroy();
    total_doc_cnt_scan_param_ = nullptr;
  }

  for (int64_t i = 0; i < inv_scan_params_.count(); ++i) {
    if (OB_NOT_NULL(inv_scan_params_[i])) {
      inv_scan_params_[i]->destroy_schema_guard();
      inv_scan_params_[i]->snapshot_.reset();
      inv_scan_params_[i]->destroy();
    }
  }
  inv_scan_params_.reset();

  for (int64_t i = 0; i < inv_agg_params_.count(); ++i) {
    if (OB_NOT_NULL(inv_agg_params_[i])) {
      inv_agg_params_[i]->destroy_schema_guard();
      inv_agg_params_[i]->snapshot_.reset();
      inv_agg_params_[i]->destroy();
    }
  }
  inv_agg_params_.reset();

  for (int64_t i = 0; i < fwd_scan_params_.count(); ++i) {
    if (OB_NOT_NULL(fwd_scan_params_[i])) {
      fwd_scan_params_[i]->destroy_schema_guard();
      fwd_scan_params_[i]->snapshot_.reset();
      fwd_scan_params_[i]->destroy();
    }
  }
  fwd_scan_params_.reset();

  for (int64_t i = 0; i < block_max_scan_params_.count(); ++i) {
    if (OB_NOT_NULL(block_max_scan_params_[i])) {
      block_max_scan_params_[i]->destroy_schema_guard();
      block_max_scan_params_[i]->snapshot_.reset();
      block_max_scan_params_[i]->destroy();
    }
  }
  block_max_scan_params_.reset();

  if (is_init_complete() && OB_NOT_NULL(boolean_compute_node_)) {
    boolean_compute_node_->release();
    boolean_compute_node_ = nullptr;
  }

  if (OB_NOT_NULL(sparse_retrieval_iter_)) {
    sparse_retrieval_iter_->reset();
    sparse_retrieval_iter_ = nullptr;
  }
  // Provider holds scan iters bound to mem_context_ — destroy before tearing
  // down mem_context_ / myself_allocator_ to release its scan iters cleanly.
  destroy_logical_stats_provider_();
  dim_iters_.reset();
  dim_iter_ = nullptr;
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  query_tokens_.reset();
  myself_allocator_.reset();
  ir_ctdef_ = nullptr;
  ir_rtdef_ = nullptr;
  tx_desc_ = nullptr;
  snapshot_ = nullptr;
  topk_limit_ = 0;
  inv_idx_tablet_switched_ = false;
  is_inited_ = false;
  // [LT plain MATCH] full release: drop runtime ns/lt so a future open re-reads
  // session (different SQL execution may carry a different @namespace_id).
  logical_table_ctx_.reset();
  runtime_ns_lt_loaded_ = false;
  return ret;
}

int ObDASTRMergeIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sparse retrieval iter is null", K(ret));
  } else {
    ret = sparse_retrieval_iter_->get_next_row();
  }
  return ret;
}

int ObDASTRMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sparse retrieval iter is null", K(ret));
  } else if (OB_UNLIKELY(0 == capacity)) {
    count = 0;
  } else {
    ret = sparse_retrieval_iter_->get_next_rows(capacity, count);
  }
  return ret;
}

int ObDASTRMergeIter::build_query_tokens(const ObDASIRScanCtDef *ir_ctdef,
                                         ObDASIRScanRtDef *ir_rtdef,
                                         common::ObIAllocator &alloc,
                                         ObArray<ObString> &query_tokens,
                                         ObFtsEvalNode *&root_node,
                                         bool &has_duplicate_boolean_tokens)
{
  int ret = OB_SUCCESS;
  has_duplicate_boolean_tokens = false;
  ObExpr *search_text = ir_ctdef->search_text_;
  ObEvalCtx *eval_ctx = ir_rtdef->eval_ctx_;
  ObDatum *search_text_datum = nullptr;
  if (OB_ISNULL(search_text) || OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(search_text), KP(eval_ctx));
  } else if (query_tokens.count() != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query tokens are not empty", K(ret));
  } else if (OB_FAIL(search_text->eval(*eval_ctx, search_text_datum))) {
    LOG_WARN("expr evaluation failed", K(ret));
  } else if (0 == search_text_datum->len_) {
    // empty query text
  } else if (BOOLEAN_MODE == ir_ctdef->mode_flag_) {
    const ObString &search_text_string = search_text_datum->get_string();
    const ObCollationType &cs_type = search_text->datum_meta_.cs_type_;
    const ObCollationType dst_type = ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI;

    ObString str_dest;
    if (cs_type != dst_type) {
      ObString tmp_out;
      if (OB_FAIL(ObCharset::tolower(cs_type, search_text_string, tmp_out, alloc))) {
        LOG_WARN("failed to casedown string", K(ret), K(cs_type), K(search_text_string));
      } else if (OB_FAIL(common::ObCharset::charset_convert(alloc, tmp_out, cs_type, dst_type, str_dest))) {
        LOG_WARN("failed to convert string", K(ret), K(cs_type), K(search_text_string));
      }
    } else if (OB_FAIL(ObCharset::tolower(cs_type, search_text_string, str_dest, alloc))){
      LOG_WARN("failed to casedown string", K(ret), K(cs_type), K(search_text_string));
    }

    void *buf = nullptr;
    FtsParserResult *fts_parser;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(buf = (&alloc)->alloc(sizeof(FtsParserResult)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(sizeof(FtsParserResult)), K(ret));
    } else {
      fts_parser = static_cast<FtsParserResult *>(buf);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(buf = (&alloc)->alloc(str_dest.length() + 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(sizeof(FtsParserResult)), K(ret));
    } else {
      MEMSET(buf, 0, str_dest.length() + 1);
      MEMCPY(buf, str_dest.ptr(), str_dest.length());
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(fts_parse_docment(static_cast<char *>(buf), str_dest.length(), &alloc, fts_parser))) {
    } else if (FTS_OK != fts_parser->ret_) {
      if (FTS_ERROR_MEMORY == fts_parser->ret_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (FTS_ERROR_SYNTAX == fts_parser->ret_) {
        ret = OB_ERR_PARSER_SYNTAX;
      } else if (FTS_ERROR_OTHER == fts_parser->ret_) {
        ret = OB_ERR_UNEXPECTED;
      }
      LOG_WARN("failed to parse query text", K(ret), K(fts_parser->err_info_.str_));
    } else if (OB_ISNULL(fts_parser->root_)) {
      // do nothing
    } else {
      FtsNode *node = fts_parser->root_;
      ObFtsEvalNode *parant_node =nullptr;
      hash::ObHashMap<ObString, int32_t> tokens_map;
      const int64_t ft_word_bkt_cnt = MAX(search_text_string.length() / 10, 2);
      if (OB_FAIL(tokens_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "FTWordMap")))) {
        LOG_WARN("failed to create token map", K(ret));
      } else if (OB_FAIL(ObFtsEvalNode::fts_boolean_node_create(parant_node, node, cs_type, alloc, query_tokens, tokens_map, has_duplicate_boolean_tokens))) {
        LOG_WARN("failed to get query tokens", K(ret));
      } else {
        root_node = parant_node;
      }
    }
    LOG_DEBUG("boolean query", K(has_duplicate_boolean_tokens), K(search_text_string), K(query_tokens));
  } else {
    // TODO: FTParseHelper currently does not support deduplicate tokens
    //       We should abstract such universal analyse functors into utility structs
    const ObString &search_text_string = search_text_datum->get_string();
    const ObString &parser_name = ir_ctdef->get_inv_idx_scan_ctdef()->table_param_.get_parser_name();
    const ObString &parser_properties = ir_ctdef->get_inv_idx_scan_ctdef()->table_param_.get_parser_property();

    const ObObjMeta &meta = search_text->obj_meta_;
    int64_t doc_length = 0;
    storage::ObFTParseHelper tokenize_helper;
    common::ObSEArray<ObFTToken, 16> tokens;
    ObFTTokenMap token_map;
    const int64_t ft_word_bkt_cnt = MAX(search_text_string.length() / 10, 2);
    if (OB_FAIL(tokenize_helper.init(&alloc, parser_name, parser_properties, share::schema::OB_FTS_INDEX_TYPE_MATCH))) {
      LOG_WARN("failed to init tokenize helper", K(ret));
    } else if (OB_FAIL(token_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "ft_token_map")))) {
      LOG_WARN("failed to create token map", K(ret));
    } else if (OB_FAIL(tokenize_helper.segment(
                           meta,
                           search_text_string.ptr(),
                           search_text_string.length(),
                           doc_length,
                           token_map))) {
      LOG_WARN("failed to segment", K(ret), K(search_text_string), K(meta), K(doc_length));
    } else {
      for (ObFTTokenMap::const_iterator iter = token_map.begin();
          OB_SUCC(ret) && iter != token_map.end();
          ++iter) {
        const ObFTToken &token = iter->first;
        ObString token_string;
        if (OB_FAIL(ob_write_string(alloc, token.get_token().get_string(), token_string))) {
          LOG_WARN("failed to deep copy query token", K(ret));
        } else if (OB_FAIL(query_tokens.push_back(token_string))) {
          LOG_WARN("failed to append query token", K(ret));
        }
      }
    }
    LOG_DEBUG("tokenized text query:", K(ret), KPC(search_text_datum), K(query_tokens));
  }
  return ret;
}

int ObDASTRMergeIter::init_topk_limit(const ObDASIRScanCtDef *ir_ctdef,
                                      ObEvalCtx &eval_ctx,
                                      int64_t &topk_limit)
{
  int ret = OB_SUCCESS;
  topk_limit = 0;
  ObExpr *topk_limit_expr = ir_ctdef->topk_limit_expr_;
  ObExpr *topk_offset_expr = ir_ctdef->topk_offset_expr_;
  ObDatum *topk_limit_datum = nullptr;
  ObDatum *topk_offset_datum = nullptr;
  if (OB_ISNULL(topk_limit_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null topk limit expr", K(ret));
  } else if (OB_FAIL(topk_limit_expr->eval(eval_ctx, topk_limit_datum))) {
    LOG_WARN("failed to eval topk limit expr", K(ret));
  } else if (OB_ISNULL(topk_limit_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null topk limit datum", K(ret));
  } else if (nullptr != topk_offset_expr
      && OB_FAIL(topk_offset_expr->eval(eval_ctx, topk_offset_datum))) {
    LOG_WARN("failed to eval topk offset expr", K(ret));
  } else if (nullptr != topk_offset_expr && OB_ISNULL(topk_offset_datum)) {
    LOG_WARN("unexpected null topk offset datum", K(ret));
  } else {
    topk_limit = (topk_limit_datum->is_null() || topk_limit_datum->get_int() < 0) ? 0 : topk_limit_datum->get_int();
    if (nullptr != topk_offset_expr) {
      topk_limit += (topk_offset_datum->is_null() || topk_offset_datum->get_int() < 0) ? 0 : topk_offset_datum->get_int();
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
