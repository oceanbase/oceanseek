/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/iter/ob_das_vec_index_scan_iter.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/das/ob_das_utils.h"
#include "sql/das/ob_das_vec_define.h"
#include "sql/das/search/ob_das_boolean_query.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/async_index/ob_async_index_segment_group.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_access_service.h"
#include "share/ob_debug_sync.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

static int add_async_index_table_id(const uint64_t table_id, ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  for (int64_t i = 0; !exists && i < table_ids.count(); ++i) {
    exists = table_ids.at(i) == table_id;
  }
  if (!exists && OB_FAIL(table_ids.push_back(table_id))) {
    LOG_WARN("failed to add async index table id", K(ret), K(table_id));
  }
  return ret;
}

static bool is_async_fts_wrapper(const ObDASBaseCtDef *ctdef)
{
  return OB_NOT_NULL(ctdef)
      && DAS_OP_BOOLEAN_QUERY == ctdef->op_type_
      && static_cast<const ObDASBooleanQueryCtDef *>(ctdef)->is_async();
}

static bool is_async_domain_transparent_op(const ObDASOpType op_type)
{
  return DAS_OP_BOOLEAN_QUERY == op_type
      || DAS_OP_FUSION_QUERY == op_type
      || DAS_OP_TOPK_COLLECT == op_type
      || DAS_OP_TABLE_LOOKUP == op_type
      || DAS_OP_IR_AUX_LOOKUP == op_type
      || DAS_OP_FUNC_LOOKUP == op_type
      || DAS_OP_INDEX_PROJ_LOOKUP == op_type
      || DAS_OP_INDEX_MERGE == op_type
      || DAS_OP_DOC_ID_MERGE == op_type
      || DAS_OP_DOMAIN_ID_MERGE == op_type;
}

static bool is_async_domain_ignored_op(const ObDASOpType op_type)
{
  return DAS_OP_VEC_SCAN == op_type
      || DAS_OP_VID_MERGE == op_type
      || DAS_OP_VEC_INDEX_DRIVER == op_type
      || DAS_OP_VEC_INDEX_SCAN == op_type
      || DAS_OP_VEC_INDEX_HNSW_SCAN == op_type
      || DAS_OP_VEC_INDEX_IVF_SCAN == op_type;
}

static bool is_async_semantic_root_op(const ObDASOpType op_type)
{
  return DAS_OP_MATCH_QUERY == op_type
      || DAS_OP_MATCH_PHRASE_QUERY == op_type
      || DAS_OP_MULTI_MATCH_QUERY == op_type
      || DAS_OP_QUERY_STRING_QUERY == op_type;
}

static bool is_supported_async_wrapper_query_op(const ObDASOpType op_type)
{
  return DAS_OP_MATCH_QUERY == op_type
      || DAS_OP_MATCH_PHRASE_QUERY == op_type
      || DAS_OP_MULTI_MATCH_QUERY == op_type
      || DAS_OP_QUERY_STRING_QUERY == op_type
      || DAS_OP_SCALAR_QUERY == op_type
      || DAS_OP_SCALAR_SCAN_QUERY == op_type
      || DAS_OP_NESTED_QUERY == op_type
      || DAS_OP_BOOLEAN_QUERY == op_type;
}

struct ObDASAsyncIndexDomainAnalyzeCtx
{
  explicit ObDASAsyncIndexDomainAnalyzeCtx(
      const ObDASScalarScanCtDef *shared_delete_ctdef = nullptr)
    : shared_delete_ctdef_(shared_delete_ctdef),
      mixed_sync_(false),
      has_group_compatible_sync_filter_(false),
      non_alignable_(false),
      has_async_fts_wrapper_(false),
      is_pure_range_subtree_(true)
  {}

  const ObDASScalarScanCtDef *shared_delete_ctdef_;
  bool mixed_sync_;
  bool has_group_compatible_sync_filter_;
  bool non_alignable_;
  bool has_async_fts_wrapper_;
  bool is_pure_range_subtree_;
};

static int validate_async_delete_scan_consistency(
    const ObDASScalarScanCtDef &left,
    const ObDASScalarScanCtDef &right)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.ref_table_id_ != right.ref_table_id_
                  || left.schema_version_ != right.schema_version_
                  || left.flags_ != right.flags_
                  || left.access_column_ids_.count() != right.access_column_ids_.count()
                  || left.result_output_.count() != right.result_output_.count()
                  || left.rowkey_exprs_.count() != right.rowkey_exprs_.count())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("async delete scan definitions are inconsistent", K(ret),
             K(left.ref_table_id_), K(right.ref_table_id_),
             K(left.schema_version_), K(right.schema_version_),
             K(left.flags_), K(right.flags_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < left.access_column_ids_.count(); ++i) {
    if (OB_UNLIKELY(left.access_column_ids_.at(i) != right.access_column_ids_.at(i))) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("async delete access columns are inconsistent", K(ret), K(i),
               K(left.access_column_ids_.at(i)), K(right.access_column_ids_.at(i)));
    }
  }
  // Runtime expressions are generated independently for each async leaf, so their
  // addresses do not participate in the canonical deletion-scan descriptor.
  return ret;
}

static int validate_async_fts_wrapper(
    const ObDASBooleanQueryCtDef &wrapper_ctdef,
    const ObDASBaseCtDef *&query_ctdef,
    ObDASAsyncIndexDomainAnalyzeCtx &analyze_ctx)
{
  int ret = OB_SUCCESS;
  ObBooleanSubClause<ObIDASSearchCtDef> must;
  ObBooleanSubClause<ObIDASSearchCtDef> filter;
  ObBooleanSubClause<ObIDASSearchCtDef> should;
  ObBooleanSubClause<ObIDASSearchCtDef> must_not;
  ObIDASSearchCtDef *query = nullptr;
  ObIDASSearchCtDef *deletion = nullptr;
  const ObDASScalarScanCtDef *delete_scan_ctdef = nullptr;
  query_ctdef = nullptr;
  if (OB_FAIL(wrapper_ctdef.must(must))) {
    LOG_WARN("failed to get async wrapper must clause", K(ret));
  } else if (OB_FAIL(wrapper_ctdef.filter(filter))) {
    LOG_WARN("failed to get async wrapper filter clause", K(ret));
  } else if (OB_FAIL(wrapper_ctdef.should(should))) {
    LOG_WARN("failed to get async wrapper should clause", K(ret));
  } else if (OB_FAIL(wrapper_ctdef.must_not(must_not))) {
    LOG_WARN("failed to get async wrapper must-not clause", K(ret));
  } else if (OB_UNLIKELY(1 != must.count() || !filter.empty()
                         || !should.empty() || 1 != must_not.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected async fts wrapper shape", K(ret), K(must.count()),
             K(filter.count()), K(should.count()), K(must_not.count()));
  } else if (OB_FAIL(must.get(0, query))) {
    LOG_WARN("failed to get async wrapper query", K(ret));
  } else if (OB_FAIL(must_not.get(0, deletion))) {
    LOG_WARN("failed to get async wrapper deletion", K(ret));
  } else if (OB_ISNULL(query) || OB_ISNULL(deletion)
             || OB_UNLIKELY(DAS_OP_SCALAR_SCAN_QUERY != deletion->op_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async fts wrapper child", K(ret), KP(query), KP(deletion));
  } else if (FALSE_IT(delete_scan_ctdef =
                 static_cast<const ObDASScalarScanCtDef *>(deletion))) {
  } else if (OB_UNLIKELY(OB_INVALID_ID == delete_scan_ctdef->ref_table_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async delete scan definition", K(ret), KPC(delete_scan_ctdef));
  } else if (OB_UNLIKELY(!is_supported_async_wrapper_query_op(query->op_type_))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported query in async fts domain", K(ret), K(query->op_type_));
  } else if (OB_ISNULL(analyze_ctx.shared_delete_ctdef_)) {
    analyze_ctx.shared_delete_ctdef_ = delete_scan_ctdef;
    query_ctdef = query;
  } else if (OB_FAIL(validate_async_delete_scan_consistency(
                 *analyze_ctx.shared_delete_ctdef_, *delete_scan_ctdef))) {
    LOG_WARN("async index wrappers have inconsistent delete scans", K(ret));
  } else {
    query_ctdef = query;
  }
  return ret;
}

static int add_async_semantic_branch(
    const uint64_t table_id,
    ObIArray<ObDASAsyncIndexSemanticBranchCount> &branch_counts)
{
  int ret = OB_SUCCESS;
  int64_t branch_idx = -1;
  for (int64_t i = 0; branch_idx < 0 && i < branch_counts.count(); ++i) {
    if (table_id == branch_counts.at(i).index_table_id_) {
      branch_idx = i;
    }
  }
  if (branch_idx >= 0) {
    ++branch_counts.at(branch_idx).branch_count_;
  } else if (OB_FAIL(branch_counts.push_back(
                 ObDASAsyncIndexSemanticBranchCount(table_id, 1)))) {
    LOG_WARN("failed to add async semantic branch", K(ret), K(table_id));
  }
  return ret;
}

static void set_async_domain_analysis(
    const ObIArray<uint64_t> &table_ids,
    const bool mixed_sync,
    const bool has_group_compatible_sync_filter,
    const bool non_alignable,
    ObDASAsyncIndexDomainAnalysis &analysis)
{
  analysis.index_table_id_ = OB_INVALID_ID;
  analysis.has_group_compatible_sync_filter_ =
      has_group_compatible_sync_filter;
  if (non_alignable) {
    analysis.type_ = ASYNC_INDEX_DOMAIN_NON_ALIGNABLE;
  } else if (mixed_sync) {
    analysis.type_ = ASYNC_INDEX_DOMAIN_MIXED_SYNC;
  } else if (table_ids.empty()) {
    analysis.type_ = ASYNC_INDEX_DOMAIN_NONE;
  } else if (1 == table_ids.count()) {
    analysis.type_ = ASYNC_INDEX_DOMAIN_SINGLE;
    analysis.index_table_id_ = table_ids.at(0);
  } else {
    analysis.type_ = ASYNC_INDEX_DOMAIN_MULTI;
  }
}

static int analyze_async_index_domain_impl(
    const ObDASBaseCtDef *ctdef,
    ObIArray<uint64_t> &table_ids,
    ObIArray<ObDASAsyncIndexSemanticBranchCount> *branch_counts,
    ObDASAsyncIndexDomainAnalyzeCtx &analyze_ctx,
    const bool in_nested_scope,
    const bool in_async_wrapper,
    const bool count_semantic_roots,
    const bool allow_sync_filter = false)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null ctdef for async index domain analysis", K(ret));
  } else if (DAS_OP_VEC_INDEX_ASYNC_IVF_SCAN == ctdef->op_type_) {
    const auto *scan = static_cast<const ObDASVecIndexAsyncIVFScanCtDef *>(ctdef)->get_index_scan_ctdef();
    if (nullptr == scan) { ret = OB_ERR_UNEXPECTED; }
    else if (OB_FAIL(add_async_index_table_id(scan->ref_table_id_, table_ids))) {
      LOG_WARN("failed to analyze async IVF logical leaf", K(ret));
    }
  } else if (DAS_OP_VEC_INDEX_DRIVER == ctdef->op_type_ && ctdef->children_cnt_ > 0
             && nullptr != ctdef->children_ && nullptr != ctdef->children_[0]
             && DAS_OP_VEC_INDEX_ASYNC_IVF_SCAN == ctdef->children_[0]->op_type_) {
    // Catalog alignment includes the logical IVF leaf and its actual filter domain.
    // Its physical index/delete children are atomic; they are not Search branches.
    for (int64_t i = 0; OB_SUCC(ret) && i < ctdef->children_cnt_; ++i) {
      if (OB_FAIL(SMART_CALL(analyze_async_index_domain_impl(ctdef->children_[i], table_ids,
          branch_counts, analyze_ctx, in_nested_scope, in_async_wrapper, count_semantic_roots)))) {
        LOG_WARN("failed to analyze async IVF driver domain", K(ret), K(i));
      }
    }
  } else if (DAS_OP_IR_SCAN == ctdef->op_type_) {
    analyze_ctx.is_pure_range_subtree_ = false;
    const ObDASIRScanCtDef *ir_ctdef = static_cast<const ObDASIRScanCtDef *>(ctdef);
    const ObDASScalarScanCtDef *inv_ctdef = ir_ctdef->get_inv_idx_scan_scalar_ctdef();
    if (!ir_ctdef->is_async_index_) {
      analyze_ctx.mixed_sync_ = true;
    } else if (OB_ISNULL(inv_ctdef)
               || OB_UNLIKELY(OB_INVALID_ID == inv_ctdef->ref_table_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async inverted index definition", K(ret), KP(inv_ctdef));
    } else if (in_nested_scope) {
      analyze_ctx.non_alignable_ = true;
    } else if (OB_FAIL(add_async_index_table_id(inv_ctdef->ref_table_id_, table_ids))) {
      LOG_WARN("failed to collect async index table id", K(ret), K(inv_ctdef->ref_table_id_));
    }
    // IR is an atomic leaf; its internal physical-scan children do not participate in
    // query-domain classification.
  } else if (DAS_OP_SCALAR_SCAN_QUERY == ctdef->op_type_) {
    const ObDASScalarScanCtDef *scalar_ctdef =
        static_cast<const ObDASScalarScanCtDef *>(ctdef);
    analyze_ctx.is_pure_range_subtree_ = analyze_ctx.is_pure_range_subtree_
        && scalar_ctdef->is_async_index_
        && !scalar_ctdef->is_async_ror_scan()
        && !scalar_ctdef->is_full_subtree_scan_for_nested_
        && !scalar_ctdef->is_data_table_universe_scan_;
    if (!scalar_ctdef->is_async_index_) {
      if (allow_sync_filter) {
        // The synchronous scalar filter may participate in Group execution.
        // Runtime planning decides between one direct consumer and a lazy
        // query-scope bitmap shared by all BMM consumers.
        analyze_ctx.has_group_compatible_sync_filter_ = true;
      } else {
        analyze_ctx.mixed_sync_ = true;
      }
    } else if (OB_UNLIKELY(OB_INVALID_ID == scalar_ctdef->ref_table_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async scalar index definition", K(ret), KPC(scalar_ctdef));
    } else if (OB_FAIL(add_async_index_table_id(
                   scalar_ctdef->ref_table_id_, table_ids))) {
      LOG_WARN("failed to collect async scalar index table id", K(ret),
               K(scalar_ctdef->ref_table_id_));
    }
  } else if (DAS_OP_SCALAR_QUERY == ctdef->op_type_) {
    // A Scalar Query stores both the index scan and the primary-table probe fallback;
    // analyze only the index execution path.
    const ObDASScalarCtDef *scalar_ctdef =
        static_cast<const ObDASScalarCtDef *>(ctdef);
    const ObDASScalarScanCtDef *index_scan_ctdef =
        scalar_ctdef->get_index_scan_ctdef();
    if (OB_ISNULL(index_scan_ctdef)) {
      analyze_ctx.is_pure_range_subtree_ = false;
      // A primary-only scalar is not a pure async range subtree. It may still
      // participate in a Group when this clause is in a synchronous Filter domain.
      if (allow_sync_filter) {
        analyze_ctx.has_group_compatible_sync_filter_ = true;
      } else {
        analyze_ctx.mixed_sync_ = true;
      }
    } else if (OB_FAIL(analyze_async_index_domain_impl(
                   index_scan_ctdef, table_ids, nullptr, analyze_ctx,
                   in_nested_scope, in_async_wrapper, false,
                   allow_sync_filter))) {
      LOG_WARN("failed to analyze scalar index scan domain", K(ret));
    }
  } else if (is_async_semantic_root_op(ctdef->op_type_)) {
    analyze_ctx.is_pure_range_subtree_ = false;
    ObSEArray<uint64_t, 2> semantic_table_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < ctdef->children_cnt_; ++i) {
      if (OB_ISNULL(ctdef->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null child in async semantic root", K(ret), K(i), K(ctdef->op_type_));
      } else if (OB_FAIL(SMART_CALL(analyze_async_index_domain_impl(
                     ctdef->children_[i], semantic_table_ids, nullptr,
                     analyze_ctx, in_nested_scope, in_async_wrapper, false)))) {
        LOG_WARN("failed to analyze async semantic root", K(ret), K(i), K(ctdef->op_type_));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < semantic_table_ids.count(); ++i) {
      if (OB_FAIL(add_async_index_table_id(semantic_table_ids.at(i), table_ids))) {
        LOG_WARN("failed to merge semantic root index", K(ret), K(i));
      } else if (count_semantic_roots && OB_NOT_NULL(branch_counts)
                 && OB_FAIL(add_async_semantic_branch(
                        semantic_table_ids.at(i), *branch_counts))) {
        LOG_WARN("failed to count semantic root index", K(ret), K(i));
      }
    }
  } else if (DAS_OP_NESTED_QUERY == ctdef->op_type_) {
    analyze_ctx.is_pure_range_subtree_ = analyze_ctx.is_pure_range_subtree_
        && 1 == ctdef->children_cnt_
        && OB_NOT_NULL(ctdef->children_);
    // Nested Search Index is supported; nested full-text remains unsupported.
    for (int64_t i = 0; OB_SUCC(ret) && i < ctdef->children_cnt_; ++i) {
      if (OB_ISNULL(ctdef->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null child in async nested domain", K(ret), K(i));
      } else if (OB_FAIL(SMART_CALL(analyze_async_index_domain_impl(
                     ctdef->children_[i], table_ids, branch_counts,
                     analyze_ctx, true /* in_nested_scope */, in_async_wrapper,
                     count_semantic_roots)))) {
        LOG_WARN("failed to analyze async nested child domain", K(ret), K(i));
      }
    }
  } else if (is_async_fts_wrapper(ctdef)) {
    analyze_ctx.is_pure_range_subtree_ = false;
    analyze_ctx.has_async_fts_wrapper_ = true;
    const ObDASBooleanQueryCtDef *boolean_ctdef =
        static_cast<const ObDASBooleanQueryCtDef *>(ctdef);
    const ObDASBaseCtDef *query_ctdef = nullptr;
    if (in_nested_scope) {
      analyze_ctx.non_alignable_ = true;
    }
    if (in_async_wrapper) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("nested async fts wrapper is not supported", K(ret));
    } else if (OB_FAIL(validate_async_fts_wrapper(
                   *boolean_ctdef, query_ctdef, analyze_ctx))) {
      LOG_WARN("failed to validate async fts wrapper", K(ret));
    } else if (OB_FAIL(analyze_async_index_domain_impl(
                   query_ctdef, table_ids, branch_counts, analyze_ctx,
                   in_nested_scope, true /* in_async_wrapper */,
                   count_semantic_roots))) {
      LOG_WARN("failed to analyze async wrapper user query", K(ret));
    }
  } else if (is_async_domain_ignored_op(ctdef->op_type_)) {
    analyze_ctx.is_pure_range_subtree_ = false;
    // Vector branches do not belong to the async full-text query domain.
  } else if (DAS_OP_BOOLEAN_QUERY == ctdef->op_type_) {
    const ObDASBooleanQueryCtDef *boolean_ctdef =
        static_cast<const ObDASBooleanQueryCtDef *>(ctdef);
    analyze_ctx.is_pure_range_subtree_ = analyze_ctx.is_pure_range_subtree_
        && !boolean_ctdef->is_async()
        && !boolean_ctdef->has_merged_probe()
        && boolean_ctdef->has_positive_clause()
        && ctdef->children_cnt_ > 0
        && OB_NOT_NULL(ctdef->children_);
    enum BooleanClauseIndex
    {
      MUST_CLAUSE_IDX = 0,
      FILTER_CLAUSE_IDX,
      SHOULD_CLAUSE_IDX,
      MUST_NOT_CLAUSE_IDX,
      CLAUSE_COUNT
    };
    ObBooleanSubClause<ObIDASSearchCtDef> clauses[CLAUSE_COUNT];
    if (OB_FAIL(boolean_ctdef->must(clauses[MUST_CLAUSE_IDX]))) {
      LOG_WARN("failed to get boolean must domain", K(ret));
    } else if (OB_FAIL(boolean_ctdef->filter(clauses[FILTER_CLAUSE_IDX]))) {
      LOG_WARN("failed to get boolean filter domain", K(ret));
    } else if (OB_FAIL(boolean_ctdef->should(clauses[SHOULD_CLAUSE_IDX]))) {
      LOG_WARN("failed to get boolean should domain", K(ret));
    } else if (OB_FAIL(boolean_ctdef->must_not(clauses[MUST_NOT_CLAUSE_IDX]))) {
      LOG_WARN("failed to get boolean must-not domain", K(ret));
    } else {
      int64_t semantic_child_cnt = 0;
      for (int64_t clause_idx = 0; clause_idx < CLAUSE_COUNT; ++clause_idx) {
        semantic_child_cnt += clauses[clause_idx].count();
      }
      if (0 == semantic_child_cnt) {
        // Preserve compatibility with lightweight CtDefs used by callers and
        // tests that only populate children_ without clause offsets.
        for (int64_t i = 0; OB_SUCC(ret) && i < ctdef->children_cnt_; ++i) {
          if (OB_ISNULL(ctdef->children_[i])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null child in boolean async domain", K(ret), K(i));
          } else if (OB_FAIL(SMART_CALL(analyze_async_index_domain_impl(
                         ctdef->children_[i], table_ids, branch_counts,
                         analyze_ctx, in_nested_scope, in_async_wrapper,
                         count_semantic_roots, allow_sync_filter)))) {
            LOG_WARN("failed to analyze boolean async child", K(ret), K(i));
          }
        }
      } else {
        for (int64_t clause_idx = 0; OB_SUCC(ret) && clause_idx < CLAUSE_COUNT; ++clause_idx) {
          const bool child_allow_sync_filter = allow_sync_filter || FILTER_CLAUSE_IDX == clause_idx;
          for (int64_t i = 0; OB_SUCC(ret) && i < clauses[clause_idx].count(); ++i) {
            ObIDASSearchCtDef *child = nullptr;
            if (OB_FAIL(clauses[clause_idx].get(i, child))) {
              LOG_WARN("failed to get boolean domain clause", K(ret), K(clause_idx), K(i));
            } else if (OB_ISNULL(child)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("null boolean domain clause", K(ret), K(clause_idx), K(i));
            } else if (OB_FAIL(SMART_CALL(analyze_async_index_domain_impl(
                           child, table_ids, branch_counts, analyze_ctx,
                           in_nested_scope, in_async_wrapper,
                           count_semantic_roots, child_allow_sync_filter)))) {
              LOG_WARN("failed to analyze boolean domain clause", K(ret),
                       K(clause_idx), K(i));
            }
          }
        }
      }
    }
  } else if (is_async_domain_transparent_op(ctdef->op_type_)) {
    analyze_ctx.is_pure_range_subtree_ = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < ctdef->children_cnt_; ++i) {
      if (OB_ISNULL(ctdef->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null child in async index domain", K(ret), K(i), K(ctdef->op_type_));
      } else if (OB_FAIL(SMART_CALL(analyze_async_index_domain_impl(
                     ctdef->children_[i], table_ids, branch_counts,
                     analyze_ctx, in_nested_scope, in_async_wrapper,
                     count_semantic_roots, allow_sync_filter)))) {
        LOG_WARN("failed to analyze async index child domain", K(ret), K(i));
      }
    }
  } else {
    analyze_ctx.is_pure_range_subtree_ = false;
    ObSEArray<uint64_t, 2> unexpected_table_ids;
    ObDASAsyncIndexDomainAnalyzeCtx child_ctx(analyze_ctx.shared_delete_ctdef_);
    for (int64_t i = 0; OB_SUCC(ret)
         && i < ctdef->children_cnt_; ++i) {
      if (OB_ISNULL(ctdef->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null child below unsupported async domain parent", K(ret), K(i),
                 K(ctdef->op_type_));
      } else if (OB_FAIL(SMART_CALL(analyze_async_index_domain_impl(
                     ctdef->children_[i], unexpected_table_ids, nullptr,
                     child_ctx, in_nested_scope, in_async_wrapper, false)))) {
        LOG_WARN("failed to inspect unsupported async domain parent", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      analyze_ctx.shared_delete_ctdef_ = child_ctx.shared_delete_ctdef_;
      analyze_ctx.has_async_fts_wrapper_ =
          analyze_ctx.has_async_fts_wrapper_ || child_ctx.has_async_fts_wrapper_;
      analyze_ctx.has_group_compatible_sync_filter_ =
          analyze_ctx.has_group_compatible_sync_filter_
          || child_ctx.has_group_compatible_sync_filter_;
      if (!unexpected_table_ids.empty()
          || child_ctx.mixed_sync_ || child_ctx.non_alignable_) {
        analyze_ctx.non_alignable_ = true;
      }
    }
  }
  return ret;
}

int ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
    const ObDASBaseCtDef *ctdef,
    ObDASAsyncIndexDomainAnalysis &analysis)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 2> table_ids;
  ObDASAsyncIndexDomainAnalyzeCtx analyze_ctx;
  analysis.type_ = ASYNC_INDEX_DOMAIN_NONE;
  analysis.index_table_id_ = OB_INVALID_ID;
  analysis.has_async_fts_wrapper_ = false;
  analysis.is_pure_range_subtree_ = false;
  analysis.has_group_compatible_sync_filter_ = false;
  analysis.semantic_branch_counts_.reset();
  if (OB_FAIL(analyze_async_index_domain_impl(
          ctdef, table_ids, &analysis.semantic_branch_counts_, analyze_ctx,
          false /* in_nested_scope */, false /* in_async_wrapper */, true))) {
    LOG_WARN("failed to analyze async index domain", K(ret));
  } else {
    set_async_domain_analysis(table_ids, analyze_ctx.mixed_sync_,
                              analyze_ctx.has_group_compatible_sync_filter_,
                              analyze_ctx.non_alignable_, analysis);
    analysis.has_async_fts_wrapper_ = analyze_ctx.has_async_fts_wrapper_;
    analysis.is_pure_range_subtree_ = analyze_ctx.is_pure_range_subtree_;
  }
  return ret;
}

int ObDASAsyncIndexQueryCtx::is_group_compatible_filter_domain(
    const ObDASBaseCtDef *ctdef,
    bool &is_compatible)
{
  int ret = OB_SUCCESS;
  ObDASGroupFilterDomainAnalysis analysis;
  is_compatible = false;
  if (OB_FAIL(analyze_group_filter_domain(ctdef, analysis))) {
    LOG_WARN("failed to analyze Group-compatible filter domain", K(ret));
  } else {
    is_compatible = analysis.is_compatible_ && analysis.has_sync_filter_;
  }
  return ret;
}

int ObDASAsyncIndexQueryCtx::analyze_group_filter_domain(
    const ObDASBaseCtDef *ctdef,
    ObDASGroupFilterDomainAnalysis &analysis)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 2> table_ids;
  ObDASAsyncIndexDomainAnalyzeCtx analyze_ctx;
  analysis = ObDASGroupFilterDomainAnalysis();
  if (OB_FAIL(analyze_async_index_domain_impl(
          ctdef, table_ids, nullptr, analyze_ctx,
          false /* in_nested_scope */, false /* in_async_wrapper */,
          false /* count_semantic_roots */, true /* allow_sync_filter */))) {
    LOG_WARN("failed to classify Group filter domain", K(ret));
  } else {
    analysis.has_sync_filter_ = analyze_ctx.has_group_compatible_sync_filter_;
    analysis.has_async_filter_ = !table_ids.empty();
    analysis.is_compatible_ = !analyze_ctx.mixed_sync_
        && !analyze_ctx.non_alignable_
        && (analysis.has_sync_filter_ || analysis.has_async_filter_);
  }
  return ret;
}

static int collect_async_wrappers(
    const ObDASBaseCtDef *ctdef,
    ObIArray<uint64_t> &table_ids,
    int64_t &wrapper_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null ctdef while collecting async wrappers", K(ret));
  } else if (DAS_OP_VEC_INDEX_ASYNC_IVF_SCAN == ctdef->op_type_) {
    const auto *scan = static_cast<const ObDASVecIndexAsyncIVFScanCtDef *>(ctdef)->get_index_scan_ctdef();
    if (nullptr == scan) { ret = OB_ERR_UNEXPECTED; }
    else if (OB_FAIL(add_async_index_table_id(scan->ref_table_id_, table_ids))) {
      LOG_WARN("failed to collect async IVF index", K(ret));
    } else { ++wrapper_cnt; }
  } else if (is_async_fts_wrapper(ctdef)) {
    ObSEArray<uint64_t, 2> wrapper_table_ids;
    ObDASAsyncIndexDomainAnalyzeCtx analyze_ctx;
    if (OB_FAIL(analyze_async_index_domain_impl(
            ctdef, wrapper_table_ids, nullptr, analyze_ctx,
            false /* in_nested_scope */, false /* in_async_wrapper */, false))) {
      LOG_WARN("failed to analyze async wrapper", K(ret));
    } else if (OB_UNLIKELY(analyze_ctx.mixed_sync_
                           || analyze_ctx.non_alignable_
                           || wrapper_table_ids.empty())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported async wrapper user query", K(ret),
               K(analyze_ctx.mixed_sync_), K(analyze_ctx.non_alignable_),
               K(wrapper_table_ids.count()));
    } else {
      ++wrapper_cnt;
      for (int64_t i = 0; OB_SUCC(ret) && i < wrapper_table_ids.count(); ++i) {
        if (OB_FAIL(add_async_index_table_id(wrapper_table_ids.at(i), table_ids))) {
          LOG_WARN("failed to append wrapper async index table id", K(ret), K(i));
        }
      }
    }
    // The wrapper's must_not is the deletion scan; do not recurse into it.
  } else if (DAS_OP_IR_SCAN == ctdef->op_type_) {
    // IR is an atomic leaf; its internal physical-scan children do not participate in
    // wrapper collection.
  } else if (DAS_OP_SCALAR_SCAN_QUERY == ctdef->op_type_
             && static_cast<const ObDASScalarScanCtDef *>(ctdef)->is_async_index_) {
    const uint64_t index_table_id =
        static_cast<const ObDASScalarScanCtDef *>(ctdef)->ref_table_id_;
    if (OB_UNLIKELY(OB_INVALID_ID == index_table_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid unwrapped async scalar index table id", K(ret),
               K(index_table_id));
    } else if (OB_FAIL(add_async_index_table_id(index_table_id, table_ids))) {
      LOG_WARN("failed to collect unwrapped async scalar index table id",
               K(ret), K(index_table_id));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctdef->children_cnt_; ++i) {
      if (OB_FAIL(SMART_CALL(collect_async_wrappers(
              ctdef->children_[i], table_ids, wrapper_cnt)))) {
        LOG_WARN("failed to collect async wrapper from child", K(ret), K(i));
      }
    }
  }
  return ret;
}

static bool is_shared_async_index_query_node(const ObDASBaseCtDef *ctdef)
{
  return DAS_OP_VEC_INDEX_ASYNC_IVF_SCAN == ctdef->op_type_ || is_async_fts_wrapper(ctdef)
      || (DAS_OP_IR_SCAN == ctdef->op_type_
          && static_cast<const ObDASIRScanCtDef *>(ctdef)->is_async_index_)
      || (DAS_OP_SCALAR_SCAN_QUERY == ctdef->op_type_
          && static_cast<const ObDASScalarScanCtDef *>(ctdef)->is_async_index_);
}

bool ObDASAsyncIndexQueryCtx::contains_shared_async_index_query(const ObDASBaseCtDef *ctdef)
{
  bool found = false;
  if (OB_NOT_NULL(ctdef)) {
    found = is_shared_async_index_query_node(ctdef);
    for (int64_t i = 0; !found && i < ctdef->children_cnt_; ++i) {
      found = contains_shared_async_index_query(ctdef->children_[i]);
    }
  }
  return found;
}

int ObDASAsyncIndexQueryCtx::check_tx_isolation(const ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null session for async index query", K(ret));
  } else {
    // Session settings can change while an existing transaction keeps its isolation level.
    const transaction::ObTxIsolationLevel isolation = session->is_in_transaction()
        ? session->get_tx_desc()->get_isolation_level() : session->get_tx_isolation();
    if (transaction::is_RR_or_SERIAL_isolevel(isolation)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
          "Async index queries under REPEATABLE READ or SERIALIZABLE isolation");
      LOG_WARN("unsupported isolation level for async index query", K(ret), K(isolation));
    }
  }
  return ret;
}

bool ObDASAsyncIndexQueryCtx::contains_async_index_access(const ObDASBaseCtDef *ctdef)
{
  bool found = false;
  if (OB_NOT_NULL(ctdef)) {
    found = is_shared_async_index_query_node(ctdef)
        || (DAS_OP_VEC_SCAN == ctdef->op_type_
            && static_cast<const ObDASVecAuxScanCtDef *>(ctdef)->is_async_index_);
    for (int64_t i = 0; !found && i < ctdef->children_cnt_; ++i) {
      found = contains_async_index_access(ctdef->children_[i]);
    }
  }
  return found;
}

int ObDASAsyncIndexQueryCtx::collect_async_index_table_ids(
    const ObDASBaseCtDef *ctdef,
    ObIArray<uint64_t> &async_table_ids)
{
  int ret = OB_SUCCESS;
  int64_t wrapper_cnt = 0;
  async_table_ids.reset();
  if (OB_ISNULL(ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null ctdef for async wrapper collection", K(ret));
  } else if (OB_FAIL(collect_async_wrappers(ctdef, async_table_ids, wrapper_cnt))) {
    LOG_WARN("failed to collect async index wrappers", K(ret));
  } else if (0 == wrapper_cnt) {
    ObDASAsyncIndexDomainAnalyzeCtx analyze_ctx;
    if (OB_FAIL(analyze_async_index_domain_impl(
            ctdef, async_table_ids, nullptr, analyze_ctx,
            false /* in_nested_scope */, false /* in_async_wrapper */, false))) {
      LOG_WARN("failed to analyze unwrapped async index query", K(ret));
    } else if (analyze_ctx.mixed_sync_ || analyze_ctx.non_alignable_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported unwrapped async index query", K(ret),
               K(analyze_ctx.mixed_sync_), K(analyze_ctx.non_alignable_));
    } else if (async_table_ids.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("async index query not found", K(ret));
    }
  } else if (async_table_ids.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async index wrapper has no logical index", K(ret), K(wrapper_cnt));
  }
  if (OB_SUCCESS != ret) {
    async_table_ids.reset();
  }
  return ret;
}

static int collect_async_delete_table_id_from_ctdef(
    const ObDASBaseCtDef *ctdef,
    uint64_t &delete_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef)) {
    // skip
  } else if (DAS_OP_VEC_INDEX_ASYNC_IVF_SCAN == ctdef->op_type_) {
    const auto *scan = static_cast<const ObDASVecIndexAsyncIVFScanCtDef *>(ctdef)->get_delete_scan_ctdef();
    if (nullptr == scan) { ret = OB_ERR_UNEXPECTED; }
    else if (OB_INVALID_ID == delete_table_id) { delete_table_id = scan->ref_table_id_; }
    else if (delete_table_id != scan->ref_table_id_) { ret = OB_ERR_UNEXPECTED; }
  } else if (DAS_OP_BOOLEAN_QUERY == ctdef->op_type_
             && static_cast<const ObDASBooleanQueryCtDef *>(ctdef)->is_async()) {
    const ObDASBooleanQueryCtDef *boolean_ctdef =
        static_cast<const ObDASBooleanQueryCtDef *>(ctdef);
    ObBooleanSubClause<ObIDASSearchCtDef> must_not_clause;
    ObIDASSearchCtDef *delete_ctdef = nullptr;
    uint64_t current_delete_table_id = OB_INVALID_ID;
    if (OB_FAIL(boolean_ctdef->must_not(must_not_clause))) {
      LOG_WARN("failed to get async delete clause", K(ret));
    } else if (OB_UNLIKELY(1 != must_not_clause.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected async delete clause count", K(ret), K(must_not_clause.count()));
    } else if (OB_FAIL(must_not_clause.get(0, delete_ctdef))) {
      LOG_WARN("failed to get async delete ctdef", K(ret));
    } else if (OB_ISNULL(delete_ctdef)
               || OB_UNLIKELY(DAS_OP_SCALAR_SCAN_QUERY != delete_ctdef->op_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected async delete ctdef", K(ret), KP(delete_ctdef));
    } else if (FALSE_IT(current_delete_table_id =
                   static_cast<const ObDASScalarScanCtDef *>(delete_ctdef)->ref_table_id_)) {
    } else if (OB_UNLIKELY(OB_INVALID_ID == current_delete_table_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async delete table id", K(ret), K(current_delete_table_id));
    } else if (OB_INVALID_ID == delete_table_id) {
      delete_table_id = current_delete_table_id;
    } else if (OB_UNLIKELY(delete_table_id != current_delete_table_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("multiple async delete tables found", K(ret), K(delete_table_id),
               K(current_delete_table_id));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && OB_NOT_NULL(ctdef)
       && i < ctdef->children_cnt_; ++i) {
    if (OB_FAIL(SMART_CALL(collect_async_delete_table_id_from_ctdef(
            ctdef->children_[i], delete_table_id)))) {
      LOG_WARN("failed to collect async delete table id from child", K(ret), K(i));
    }
  }
  return ret;
}

int ObDASAsyncIndexQueryCtx::collect_async_delete_table_id(
    const ObDASBaseCtDef *ctdef,
    uint64_t &delete_table_id)
{
  int ret = OB_SUCCESS;
  delete_table_id = OB_INVALID_ID;
  if (OB_ISNULL(ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null ctdef for async delete table collection", K(ret));
  } else if (OB_FAIL(collect_async_delete_table_id_from_ctdef(
                 ctdef, delete_table_id))) {
    LOG_WARN("failed to collect async delete table id", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == delete_table_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async delete table not found", K(ret));
  }
  return ret;
}

void ObDASAsyncIndexQueryCtx::reset()
{
  for (int64_t i = 0; i < partitions_.count(); ++i) {
    if (OB_NOT_NULL(partitions_.at(i))) {
      partitions_.at(i)->~ObAsyncIndexPartitionSegCtx();
    }
  }
  partitions_.reset();
  group_alignment_enabled_ = false;
  mock_fetcher_ = nullptr;
  mock_fetcher_arg_ = nullptr;
  is_inited_ = false;
  allocator_.reset();
}

int ObDASAsyncIndexQueryCtx::build_canonical_index_table_ids_(
    const ObIArray<uint64_t> &index_table_ids,
    ObIArray<uint64_t> &canonical_ids)
{
  int ret = OB_SUCCESS;
  canonical_ids.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < index_table_ids.count(); ++i) {
    if (OB_FAIL(add_async_index_table_id(index_table_ids.at(i), canonical_ids))) {
      LOG_WARN("failed to canonicalize async index table id", K(ret), K(i));
    }
  }
  for (int64_t i = 1; i < canonical_ids.count(); ++i) {
    const uint64_t table_id = canonical_ids.at(i);
    int64_t pos = i;
    while (pos > 0 && table_id < canonical_ids.at(pos - 1)) {
      canonical_ids.at(pos) = canonical_ids.at(pos - 1);
      --pos;
    }
    canonical_ids.at(pos) = table_id;
  }
  return ret;
}

int ObDASAsyncIndexQueryCtx::build_normalized_catalog_input_(
    const ObIArray<const ObDASTabletLoc *> &tablet_locs,
    const ObIArray<uint64_t> &index_table_ids,
    const uint64_t delete_table_id,
    ObIArray<ObDASAsyncIndexCandidate> &candidates,
    ObIArray<ObDASAsyncIndexBinding> &bindings)
{
  int ret = OB_SUCCESS;
  candidates.reset();
  bindings.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_locs.count(); ++i) {
    const ObDASTabletLoc *tablet_loc = tablet_locs.at(i);
    ObDASTabletLoc *delete_loc = nullptr;
    if (OB_ISNULL(tablet_loc)
        || OB_UNLIKELY(!tablet_loc->ls_id_.is_valid()
                       || !tablet_loc->tablet_id_.is_valid()
                       || OB_ISNULL(tablet_loc->loc_meta_))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async index candidate", K(ret), K(i), KP(tablet_loc));
    } else if (OB_ISNULL(delete_loc =
                   ObDASUtils::get_related_tablet_loc(*tablet_loc, delete_table_id))
               || OB_UNLIKELY(!delete_loc->tablet_id_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("async delete tablet location not found", K(ret), K(delete_table_id),
               KPC(tablet_loc));
    } else {
      const ObDASAsyncIndexCandidate candidate(
          tablet_loc->ls_id_, tablet_loc->tablet_id_, delete_loc->tablet_id_);
      int64_t candidate_idx = -1;
      for (int64_t j = 0; candidate_idx < 0 && j < candidates.count(); ++j) {
        if (candidate.has_same_key(candidates.at(j))) {
          candidate_idx = j;
        }
      }
      if (candidate_idx >= 0 && OB_UNLIKELY(!(candidate == candidates.at(candidate_idx)))) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("conflicting async deletion tablet binding", K(ret), K(candidate),
                 "existing_candidate", candidates.at(candidate_idx));
      } else if (candidate_idx < 0 && OB_FAIL(candidates.push_back(candidate))) {
        LOG_WARN("failed to append async index candidate", K(ret), K(candidate));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < index_table_ids.count(); ++j) {
        ObDASTabletLoc *index_loc = ObDASUtils::get_related_tablet_loc(
            *tablet_loc, index_table_ids.at(j));
        if (OB_ISNULL(index_loc) || OB_UNLIKELY(!index_loc->tablet_id_.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("async index tablet location not found", K(ret),
                   "index_table_id", index_table_ids.at(j), KPC(tablet_loc));
        } else {
          const ObDASAsyncIndexBinding binding(
              tablet_loc->ls_id_, tablet_loc->tablet_id_,
              index_table_ids.at(j), index_loc->tablet_id_);
          int64_t binding_idx = -1;
          for (int64_t k = 0; binding_idx < 0 && k < bindings.count(); ++k) {
            if (binding.has_same_key(bindings.at(k))) {
              binding_idx = k;
            }
          }
          if (binding_idx >= 0 && OB_UNLIKELY(!(binding == bindings.at(binding_idx)))) {
            ret = OB_STATE_NOT_MATCH;
            LOG_WARN("conflicting async physical index binding", K(ret), K(binding),
                     "existing_binding", bindings.at(binding_idx));
          } else if (binding_idx < 0 && OB_FAIL(bindings.push_back(binding))) {
            LOG_WARN("failed to append async physical index binding", K(ret), K(binding));
          }
        }
      }
    }
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < candidates.count(); ++i) {
    const ObDASAsyncIndexCandidate candidate = candidates.at(i);
    int64_t pos = i;
    while (pos > 0 && candidate.key_less(candidates.at(pos - 1))) {
      candidates.at(pos) = candidates.at(pos - 1);
      --pos;
    }
    candidates.at(pos) = candidate;
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < bindings.count(); ++i) {
    const ObDASAsyncIndexBinding binding = bindings.at(i);
    int64_t pos = i;
    while (pos > 0 && binding.key_less(bindings.at(pos - 1))) {
      bindings.at(pos) = bindings.at(pos - 1);
      --pos;
    }
    bindings.at(pos) = binding;
  }
  return ret;
}

int ObDASAsyncIndexQueryCtx::build_normalized_resolved_input_(
    const ObLSID &ls_id,
    const ObTabletID &main_tablet_id,
    const ObIArray<uint64_t> &index_table_ids,
    const ObIArray<ObTabletID> &index_tablet_ids,
    const ObTabletID &delete_tablet_id,
    ObIArray<ObDASAsyncIndexCandidate> &candidates,
    ObIArray<ObDASAsyncIndexBinding> &bindings)
{
  int ret = OB_SUCCESS;
  candidates.reset();
  bindings.reset();
  if (OB_UNLIKELY(index_table_ids.empty()
                  || index_table_ids.count() != index_tablet_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resolved async index binding count", K(ret),
             K(index_table_ids.count()), K(index_tablet_ids.count()));
  } else if (OB_FAIL(candidates.push_back(
                 ObDASAsyncIndexCandidate(ls_id, main_tablet_id, delete_tablet_id)))) {
    LOG_WARN("failed to append resolved async index candidate", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_table_ids.count(); ++i) {
    const ObDASAsyncIndexBinding binding(
        ls_id, main_tablet_id, index_table_ids.at(i), index_tablet_ids.at(i));
    int64_t binding_idx = -1;
    if (OB_UNLIKELY(OB_INVALID_ID == binding.index_table_id_
                    || !binding.index_tablet_id_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid resolved async index binding", K(ret), K(binding), K(i));
    }
    for (int64_t j = 0; OB_SUCC(ret) && binding_idx < 0 && j < bindings.count(); ++j) {
      if (binding.has_same_key(bindings.at(j))) {
        binding_idx = j;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (binding_idx >= 0 && OB_UNLIKELY(!(binding == bindings.at(binding_idx)))) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("conflicting resolved async index binding", K(ret), K(binding),
               "existing_binding", bindings.at(binding_idx));
    } else if (binding_idx < 0 && OB_FAIL(bindings.push_back(binding))) {
      LOG_WARN("failed to append resolved async index binding", K(ret), K(binding));
    }
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < bindings.count(); ++i) {
    const ObDASAsyncIndexBinding binding = bindings.at(i);
    int64_t pos = i;
    while (pos > 0 && binding.key_less(bindings.at(pos - 1))) {
      bindings.at(pos) = bindings.at(pos - 1);
      --pos;
    }
    bindings.at(pos) = binding;
  }
  return ret;
}

int ObDASAsyncIndexQueryCtx::restore_candidate_bindings_(
    const ObDASAsyncIndexCandidate &candidate,
    const ObIArray<ObDASAsyncIndexBinding> &bindings,
    ObIArray<uint64_t> &index_table_ids,
    ObIArray<ObTabletID> &index_tablet_ids)
{
  int ret = OB_SUCCESS;
  index_table_ids.reset();
  index_tablet_ids.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < bindings.count(); ++i) {
    if (candidate.ls_id_ == bindings.at(i).ls_id_
        && candidate.main_tablet_id_ == bindings.at(i).main_tablet_id_) {
      if (OB_FAIL(index_table_ids.push_back(bindings.at(i).index_table_id_))) {
        LOG_WARN("failed to restore async logical index binding", K(ret), K(i));
      } else if (OB_FAIL(index_tablet_ids.push_back(bindings.at(i).index_tablet_id_))) {
        LOG_WARN("failed to restore async physical index binding", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(index_table_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("candidate has no async index binding", K(ret), K(candidate));
  }
  return ret;
}

int ObDASAsyncIndexQueryCtx::prepare(
    const ObDASBaseCtDef *ctdef,
    const ObIArray<const ObDASTabletLoc *> &tablet_locs,
    const SCN &sql_read_snapshot,
    const ObDASAsyncIndexExecMode exec_mode)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 2> index_table_ids;
  ObSEArray<uint64_t, 2> canonical_index_table_ids;
  ObSEArray<ObDASAsyncIndexCandidate, 4> candidates;
  ObSEArray<ObDASAsyncIndexBinding, 8> bindings;
  uint64_t delete_table_id = OB_INVALID_ID;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("async index query catalog prepared twice", K(ret));
  } else if (OB_ISNULL(ctdef) || OB_UNLIKELY(tablet_locs.empty()
                                      || !sql_read_snapshot.is_valid()
                                      || (ASYNC_INDEX_EXEC_LEGACY != exec_mode
                                          && ASYNC_INDEX_EXEC_GROUP != exec_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async index query catalog arguments", K(ret), KP(ctdef),
             K(tablet_locs.count()), K(sql_read_snapshot), K(exec_mode));
  } else if (OB_FAIL(collect_async_index_table_ids(ctdef, index_table_ids))) {
    LOG_WARN("failed to collect async index table ids", K(ret));
  } else if (OB_FAIL(build_canonical_index_table_ids_(
                 index_table_ids, canonical_index_table_ids))) {
    LOG_WARN("failed to canonicalize async index table ids", K(ret));
  } else if (OB_FAIL(collect_async_delete_table_id(ctdef, delete_table_id))) {
    LOG_WARN("failed to collect async delete table id", K(ret));
  } else if (OB_FAIL(build_normalized_catalog_input_(
                 tablet_locs, canonical_index_table_ids, delete_table_id,
                 candidates, bindings))) {
    LOG_WARN("failed to normalize async index catalog input", K(ret));
  } else {
    ObSEArray<uint64_t, 2> candidate_index_table_ids;
    ObSEArray<ObTabletID, 2> index_tablet_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates.count(); ++i) {
      if (OB_FAIL(restore_candidate_bindings_(
              candidates.at(i), bindings, candidate_index_table_ids,
              index_tablet_ids))) {
        LOG_WARN("failed to restore canonical async index bindings", K(ret), K(i));
      } else if (OB_UNLIKELY(candidate_index_table_ids.count()
                             != canonical_index_table_ids.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("incomplete canonical async index bindings", K(ret), K(i),
                 K(candidate_index_table_ids), K(canonical_index_table_ids));
      } else if (OB_FAIL(add_partition(
                     candidates.at(i).ls_id_, candidates.at(i).main_tablet_id_,
                     candidate_index_table_ids, index_tablet_ids,
                     candidates.at(i).delete_tablet_id_, sql_read_snapshot))) {
        LOG_WARN("failed to add async index query partition", K(ret),
                 "candidate", candidates.at(i));
      }
    }
    if (OB_SUCC(ret)) {
      group_alignment_enabled_ = ASYNC_INDEX_EXEC_GROUP == exec_mode;
    }
    if (OB_SUCC(ret) && OB_FAIL(finalize())) {
      LOG_WARN("failed to finalize async index query catalog", K(ret));
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

int ObDASAsyncIndexQueryCtx::prepare_resolved_partition(
    const ObDASBaseCtDef *ctdef,
    const ObLSID &ls_id,
    const ObTabletID &main_tablet_id,
    const ObIArray<uint64_t> &index_table_ids,
    const ObIArray<ObTabletID> &index_tablet_ids,
    const ObTabletID &delete_tablet_id,
    const SCN &sql_read_snapshot,
    const ObDASAsyncIndexExecMode exec_mode)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 2> ctdef_index_table_ids;
  ObSEArray<uint64_t, 2> canonical_ctdef_index_table_ids;
  ObSEArray<ObDASAsyncIndexCandidate, 1> candidates;
  ObSEArray<ObDASAsyncIndexBinding, 2> bindings;
  ObSEArray<uint64_t, 2> canonical_index_table_ids;
  ObSEArray<ObTabletID, 2> canonical_index_tablet_ids;
  uint64_t logical_delete_table_id = OB_INVALID_ID;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("resolved async index query catalog prepared twice", K(ret));
  } else if (OB_ISNULL(ctdef) || OB_UNLIKELY(!ls_id.is_valid() || !main_tablet_id.is_valid()
                                      || index_table_ids.empty()
                                      || index_table_ids.count() != index_tablet_ids.count()
                                      || !delete_tablet_id.is_valid()
                                      || !sql_read_snapshot.is_valid()
                                      || (ASYNC_INDEX_EXEC_LEGACY != exec_mode
                                          && ASYNC_INDEX_EXEC_GROUP != exec_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resolved async index query arguments", K(ret), KP(ctdef),
             K(ls_id), K(main_tablet_id), K(index_table_ids.count()),
             K(index_tablet_ids.count()), K(delete_tablet_id), K(sql_read_snapshot));
  } else if (OB_FAIL(collect_async_index_table_ids(ctdef, ctdef_index_table_ids))) {
    LOG_WARN("failed to collect remote async index ids", K(ret));
  } else if (OB_FAIL(build_canonical_index_table_ids_(
                 ctdef_index_table_ids, canonical_ctdef_index_table_ids))) {
    LOG_WARN("failed to canonicalize ctdef async index ids", K(ret));
  } else if (OB_FAIL(collect_async_delete_table_id(ctdef, logical_delete_table_id))) {
    LOG_WARN("failed to collect remote async delete id", K(ret));
  } else if (OB_FAIL(build_normalized_resolved_input_(
                 ls_id, main_tablet_id, index_table_ids, index_tablet_ids,
                 delete_tablet_id, candidates, bindings))) {
    LOG_WARN("failed to normalize resolved async index input", K(ret));
  } else if (OB_FAIL(restore_candidate_bindings_(
                 candidates.at(0), bindings, canonical_index_table_ids,
                 canonical_index_tablet_ids))) {
    LOG_WARN("failed to restore resolved async index bindings", K(ret));
  } else if (OB_UNLIKELY(canonical_ctdef_index_table_ids.count()
                         != canonical_index_table_ids.count())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("resolved async index set mismatches ctdef", K(ret),
             K(canonical_ctdef_index_table_ids), K(canonical_index_table_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < canonical_index_table_ids.count(); ++i) {
      if (OB_UNLIKELY(canonical_ctdef_index_table_ids.at(i)
                      != canonical_index_table_ids.at(i))) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("resolved async logical index mismatches ctdef", K(ret), K(i),
                 K(canonical_ctdef_index_table_ids), K(canonical_index_table_ids));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(add_partition(
                 ls_id, main_tablet_id, canonical_index_table_ids,
                 canonical_index_tablet_ids, delete_tablet_id,
                 sql_read_snapshot))) {
    LOG_WARN("failed to add resolved remote async index partition", K(ret));
  } else if (OB_SUCC(ret)) {
    group_alignment_enabled_ = ASYNC_INDEX_EXEC_GROUP == exec_mode;
  }
  if (OB_SUCC(ret) && OB_FAIL(finalize())) {
    LOG_WARN("failed to finalize remote async index catalog", K(ret));
  }
  if (OB_FAIL(ret) && !is_inited_) {
    reset();
  }
  return ret;
}

int ObDASAsyncIndexQueryCtx::add_partition(
    const ObLSID &ls_id,
    const ObTabletID &main_tablet_id,
    const ObIArray<uint64_t> &index_table_ids,
    const ObIArray<ObTabletID> &index_tablet_ids,
    const ObTabletID &delete_tablet_id,
    const SCN &sql_read_snapshot)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObAsyncIndexPartitionSegCtx *seg_ctx = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("async index query catalog is already finalized", K(ret), K(main_tablet_id));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !main_tablet_id.is_valid()
                         || index_table_ids.empty()
                         || index_table_ids.count() != index_tablet_ids.count()
                         || !delete_tablet_id.is_valid()
                         || !sql_read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(main_tablet_id),
             K(index_table_ids.count()), K(index_tablet_ids.count()),
             K(delete_tablet_id), K(sql_read_snapshot));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObAsyncIndexPartitionSegCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc partition seg ctx", K(ret));
  } else {
    seg_ctx = new (buf) ObAsyncIndexPartitionSegCtx();
    seg_ctx->ls_id_ = ls_id;
    seg_ctx->main_tablet_id_ = main_tablet_id;
    seg_ctx->sql_read_snapshot_ = sql_read_snapshot;
    if (OB_FAIL(seg_ctx->index_table_ids_.assign(index_table_ids))) {
      LOG_WARN("failed to copy async logical index ids", K(ret));
    } else if (OB_FAIL(seg_ctx->index_tablet_ids_.assign(index_tablet_ids))) {
      LOG_WARN("failed to copy async index tablet ids", K(ret));
    } else if (OB_NOT_NULL(mock_fetcher_)) {
      if (OB_FAIL(mock_fetcher_(mock_fetcher_arg_, ls_id, index_table_ids, index_tablet_ids,
                               delete_tablet_id, sql_read_snapshot, *seg_ctx))) {
        LOG_WARN("mock fetch async index segment ctx failed", K(ret), K(ls_id), K(main_tablet_id));
      }
    } else if (OB_FAIL(default_fetch_(ls_id, index_tablet_ids, delete_tablet_id,
                                     sql_read_snapshot, *seg_ctx))) {
      LOG_WARN("fetch async index segment ctx failed", K(ret), K(ls_id), K(main_tablet_id));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partitions_.push_back(seg_ctx))) {
        LOG_WARN("failed to push back partition seg ctx", K(ret));
      } else {
        LOG_INFO("[ASYNC_INDEX_QUERY] add_partition", K(ls_id), K(main_tablet_id),
            "index_count", seg_ctx->index_table_ids_.count());
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(seg_ctx)) {
      seg_ctx->~ObAsyncIndexPartitionSegCtx();
    }
  }
  return ret;
}

int ObDASAsyncIndexQueryCtx::default_fetch_(
    const ObLSID &ls_id,
    const ObIArray<ObTabletID> &index_tablet_ids,
    const ObTabletID &delete_tablet_id,
    const SCN &sql_read_snapshot,
    ObAsyncIndexPartitionSegCtx &out_seg_ctx)
{
  int ret = OB_SUCCESS;
  // Hold the search context before it resolves the per-partition async read
  // snapshot. Used by the refresh-window concurrency test.
  DEBUG_SYNC(BEFORE_ASYNC_SEARCH_SEGMENT_GROUP_CTX);
  storage::ObAccessService *tsc_service = MTL(storage::ObAccessService *);
  if (OB_ISNULL(tsc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr access service", K(ret));
  } else if (OB_FAIL(tsc_service->get_async_index_segment_group_ctx(
      ls_id, index_tablet_ids, delete_tablet_id, sql_read_snapshot,
      out_seg_ctx.storage_group_ctx_))) {
    LOG_WARN("failed to get async index segment group ctx", K(ret), K(ls_id));
  }
  return ret;
}

int ObDASAsyncIndexQueryCtx::finalize()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(partitions_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no partition collected for async index query ctx", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions_.count(); ++i) {
      if (OB_ISNULL(partitions_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null async index partition", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      LOG_INFO("[ASYNC_INDEX_QUERY] finalize", "partition_count", partitions_.count(),
          K_(group_alignment_enabled));
    }
  }
  return ret;
}

int64_t ObDASAsyncIndexQueryCtx::get_max_segment_cnt(const uint64_t index_table_id) const
{
  int64_t max_cnt = 0;
  for (int64_t i = 0; i < partitions_.count(); ++i) {
    int64_t segment_cnt = 0;
    if (OB_NOT_NULL(partitions_.at(i))
        && OB_SUCCESS == partitions_.at(i)->get_index_segment_cnt(
                             index_table_id, segment_cnt)) {
      max_cnt = MAX(max_cnt, segment_cnt);
    }
  }
  return max_cnt;
}

int64_t ObDASAsyncIndexQueryCtx::get_max_group_cnt() const
{
  int64_t max_cnt = 0;
  for (int64_t i = 0; i < partitions_.count(); ++i) {
    if (OB_NOT_NULL(partitions_.at(i))) {
      max_cnt = MAX(max_cnt,
                    partitions_.at(i)->get_storage_group_ctx().get_group_count());
    }
  }
  return max_cnt;
}

int64_t ObDASAsyncIndexQueryCtx::get_max_segment_cnt(
    const int64_t group_slot,
    const uint64_t index_table_id) const
{
  int64_t max_cnt = 0;
  for (int64_t i = 0; i < partitions_.count(); ++i) {
    const ObAsyncIndexPartitionSegCtx *partition = partitions_.at(i);
    int64_t tablet_idx = -1;
    if (OB_NOT_NULL(partition)
        && group_slot >= 0
        && group_slot < partition->get_storage_group_ctx().get_group_count()
        && OB_SUCCESS == partition->find_index_tablet_idx(index_table_id, tablet_idx)) {
      const storage::ObAsyncIndexSegmentGroup *group =
          partition->get_storage_group_ctx().get_group(group_slot);
      if (OB_NOT_NULL(group) && tablet_idx >= 0 && tablet_idx < group->get_tablet_cnt()) {
        max_cnt = MAX(max_cnt, group->get_tablet_version_ranges(tablet_idx).count());
      }
    }
  }
  return max_cnt;
}

int ObDASAsyncIndexQueryCtx::find_partition(
    const ObTabletID &main_tablet_id,
    const ObAsyncIndexPartitionSegCtx *&partition) const
{
  int ret = OB_SUCCESS;
  partition = nullptr;
  for (int64_t i = 0; OB_ISNULL(partition) && i < partitions_.count(); ++i) {
    if (OB_NOT_NULL(partitions_.at(i))
        && partitions_.at(i)->main_tablet_id_ == main_tablet_id) {
      partition = partitions_.at(i);
    }
  }
  if (OB_ISNULL(partition)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("partition not found in immutable async index query ctx",
             K(ret), K(main_tablet_id));
  }
  return ret;
}

int ObAsyncIndexPartitionSegCtx::find_index_tablet_idx(
    const uint64_t index_table_id,
    int64_t &tablet_idx) const
{
  int ret = OB_SUCCESS;
  tablet_idx = -1;
  if (OB_UNLIKELY(OB_INVALID_ID == index_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async logical index table id", K(ret), K(index_table_id));
  } else if (OB_UNLIKELY(index_table_ids_.count() != index_tablet_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async index id arrays mismatch", K(ret), K(index_table_ids_.count()),
             K(index_tablet_ids_.count()));
  } else {
    for (int64_t i = 0; tablet_idx < 0 && i < index_table_ids_.count(); ++i) {
      if (index_table_ids_.at(i) == index_table_id) {
        tablet_idx = i;
      }
    }
    if (tablet_idx < 0) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("async logical index tablet not found", K(ret), K(index_table_id),
               K_(main_tablet_id));
    }
  }
  return ret;
}

int ObAsyncIndexPartitionSegCtx::find_index_tablet_idx(
    const ObTabletID &index_tablet_id,
    int64_t &tablet_idx) const
{
  int ret = OB_SUCCESS;
  tablet_idx = -1;
  if (OB_UNLIKELY(!index_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async index tablet id", K(ret), K(index_tablet_id));
  } else {
    for (int64_t i = 0; tablet_idx < 0 && i < index_tablet_ids_.count(); ++i) {
      if (index_tablet_ids_.at(i) == index_tablet_id) {
        tablet_idx = i;
      }
    }
    if (tablet_idx < 0) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("async index tablet not found", K(ret), K(index_tablet_id),
               K_(main_tablet_id));
    }
  }
  return ret;
}

int ObAsyncIndexPartitionSegCtx::get_index_segment_cnt(
    const uint64_t index_table_id,
    int64_t &segment_cnt) const
{
  int ret = OB_SUCCESS;
  int64_t tablet_idx = -1;
  segment_cnt = 0;
  if (OB_FAIL(find_index_tablet_idx(index_table_id, tablet_idx))) {
    LOG_WARN("failed to find async index tablet ordinal", K(ret), K(index_table_id));
  }
  for (int64_t g = 0; OB_SUCC(ret) && g < storage_group_ctx_.get_group_count(); ++g) {
    const storage::ObAsyncIndexSegmentGroup *group = storage_group_ctx_.get_group(g);
    if (OB_ISNULL(group) || OB_UNLIKELY(tablet_idx >= group->get_tablet_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async index group for segment count", K(ret), K(g), K(tablet_idx), KP(group));
    } else {
      segment_cnt += group->get_tablet_version_ranges(tablet_idx).count();
    }
  }
  return ret;
}

int ObAsyncIndexPartitionSegCtx::get_pinned_index_tablet_handle(
    const ObTabletID &tablet_id,
    const storage::ObTabletHandle *&tablet_handle) const
{
  int ret = OB_SUCCESS;
  int64_t tablet_idx = -1;
  tablet_handle = nullptr;
  if (OB_FAIL(find_index_tablet_idx(tablet_id, tablet_idx))) {
    LOG_WARN("failed to find pinned async index tablet ordinal", K(ret), K(tablet_id));
  } else {
    tablet_handle = storage_group_ctx_.get_index_tablet_handle(tablet_idx);
  }
  return ret;
}

int ObAsyncIndexPartitionSegCtx::get_segment_rowid_range(
    const uint64_t index_table_id,
    const ObNewVersionRange &version_range,
    bool &available,
    uint64_t &min_rowid,
    uint64_t &max_rowid) const
{
  int ret = OB_SUCCESS;
  int64_t tablet_idx = -1;
  const storage::ObTabletHandle *tablet_handle = nullptr;
  storage::ObTableHandleV2 table_handle;
  blocksstable::ObSSTable *sstable = nullptr;
  ObNewVersionRange actual_version_range;
  storage::ObITable::TableType table_type = storage::ObITable::MAX_TABLE_TYPE;
  available = false;
  min_rowid = UINT64_MAX;
  max_rowid = 0;
  if (OB_UNLIKELY(!version_range.is_valid() || !sql_read_snapshot_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async segment lookup argument", K(ret), K(version_range),
        K_(sql_read_snapshot));
  } else if (OB_FAIL(find_index_tablet_idx(index_table_id, tablet_idx))) {
    LOG_WARN("failed to resolve async segment tablet", K(ret), K(index_table_id));
  } else if (OB_FAIL(get_pinned_index_tablet_handle(
                 index_tablet_ids_.at(tablet_idx), tablet_handle))) {
    LOG_WARN("failed to get pinned async segment tablet", K(ret), K(index_table_id));
  } else if (OB_ISNULL(tablet_handle)
             || OB_UNLIKELY(!tablet_handle->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pinned async segment tablet handle", K(ret), K(index_table_id), K(tablet_idx));
  } else if (OB_UNLIKELY(tablet_handle->get_obj()->get_tablet_id()
                        != index_tablet_ids_.at(tablet_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pinned async segment tablet does not match binding", K(ret), K(index_table_id));
  } else {
    table_type = 0 == version_range.base_version_
        ? storage::ObITable::INV_INDEX_SSTABLE : storage::ObITable::INC_INV_INDEX_SSTABLE;
    if (OB_FAIL(tablet_handle->get_obj()->get_async_index_sstable(
        table_type, version_range, table_handle))) {
      LOG_WARN("failed to get async segment range table", K(ret), K(table_type),
          K(version_range), K_(sql_read_snapshot));
    } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
      LOG_WARN("failed to get async segment range sstable", K(ret), K(table_type),
          K(version_range));
    } else if (OB_ISNULL(sstable)
               || OB_UNLIKELY(!sstable->is_inv_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected async segment range table", K(ret), K(table_type),
          K(version_range), KP(sstable));
    } else if (OB_FAIL(sstable->get_async_index_version_range(actual_version_range))) {
      LOG_WARN("failed to get async segment logical version range", K(ret), K(table_type),
          K(version_range), "table_key", sstable->get_key());
    } else if (OB_UNLIKELY(actual_version_range != version_range)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected async segment logical version range", K(ret), K(version_range),
          K(actual_version_range), "table_key", sstable->get_key());
    } else {
      const blocksstable::ObInvIdxStatsMeta &meta =
          static_cast<const blocksstable::ObInvSSTable *>(sstable)->get_inv_meta().get_stats();
      // Validate structural metadata separately from DocID layout support so valid
      // unsupported layouts can keep the heap-merge fallback.
      if (blocksstable::INV_IDX_TYPE_INVALID == meta.index_type_
          && meta.doc_id_obj_meta_.is_null()
          && UINT64_MAX == meta.min_doc_id_ && 0 == meta.max_doc_id_
          && sstable->is_empty()) {
        // Empty baseline creation leaves inv_meta reset, without schema types.
        // Require independent physical emptiness before omitting this Segment.
        available = true;
      } else if (OB_UNLIKELY(meta.index_type_ <= blocksstable::INV_IDX_TYPE_INVALID
          || meta.index_type_ >= blocksstable::INV_IDX_TYPE_MAX
          || !meta.doc_id_obj_meta_.is_valid()
          || meta.doc_id_obj_meta_.is_null()
          || (blocksstable::INV_IDX_TYPE_FTS == meta.index_type_) != meta.has_relevance_params_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid async segment range metadata", K(ret), K(version_range), "table_key", sstable->get_key());
      } else if (!meta.doc_id_obj_meta_.is_uint64() || meta.has_nested_key_) {
        // A valid but unsupported DocID layout cannot prove UINT64 output order; keep heap merge.
        LOG_TRACE("unsupported async segment range metadata", K(version_range), "table_key", sstable->get_key(), K(meta));
      } else if (OB_UNLIKELY(meta.is_empty_range()
                 && (UINT64_MAX != meta.min_doc_id_ || 0 != meta.max_doc_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid async segment empty range", K(ret), K(version_range), "table_key", sstable->get_key(),
            K(meta.min_doc_id_), K(meta.max_doc_id_));
      } else if (meta.is_empty_range() && !sstable->is_empty()) {
        // An empty statistics sentinel alone cannot prove that posting rows can be omitted.
        LOG_WARN("unproven empty async segment range, keep heap merge", K(version_range), "table_key", sstable->get_key(), K(meta));
      } else {
        available = true;
        min_rowid = meta.min_doc_id_;
        max_rowid = meta.max_doc_id_;
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
