/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/logical_table/ob_logical_table_define.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase {
namespace share {

// ---- Static col_desc arrays per aux_type ----
// Column order matches the key encoding order. All types are deterministic.

// LT_AUX_PRIMARY: [rowid(uint64)]
static const ObLogicalTableKeyColDesc LT_AUX_PRIMARY_COLS[] = {
  {common::ObUInt64Type, false},
};

// LT_AUX_SEARCH_INDEX: [col_idx(uint64), path(varchar), value(varchar), doc_id(uint64)]
static const ObLogicalTableKeyColDesc LT_AUX_SEARCH_INDEX_COLS[] = {
  {common::ObIntType,  false, false},
  {common::ObVarcharType, true,  true},
  {common::ObVarcharType, true,  true},
  {common::ObUInt64Type,  false, false},
};

// LT_AUX_FTS_INVERTED: [token(varchar), rowid(uint64)]
static const ObLogicalTableKeyColDesc LT_AUX_FTS_INVERTED_COLS[] = {
  {common::ObVarcharType, false},
  {common::ObUInt64Type,  false},
};

// LT_AUX_FTS_FORWARD: [rowid(uint64), token(varchar)]
static const ObLogicalTableKeyColDesc LT_AUX_FTS_FORWARD_COLS[] = {
  {common::ObUInt64Type, false},
  {common::ObVarcharType, false},
};

// LT_AUX_IVF_CID_VECTOR: [cid(16B ObCenterId, order-preserving)]
static const ObLogicalTableKeyColDesc LT_AUX_IVF_CID_VECTOR_COLS[] = {
  {common::ObVarcharType, false, true, OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH},
};

// LT_AUX_IVF_CID_ROWID: [cid(16B ObCenterId, order-preserving), rowid(uint64)]
static const ObLogicalTableKeyColDesc LT_AUX_IVF_CID_ROWID_COLS[] = {
  {common::ObVarcharType, false, true, OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH},
  {common::ObUInt64Type,  false},
};

// LT_AUX_IVF_ROWID_CID: [rowid(uint64), cid(16B ObCenterId, order-preserving)]
static const ObLogicalTableKeyColDesc LT_AUX_IVF_ROWID_CID_COLS[] = {
  {common::ObUInt64Type,  false},
  {common::ObVarcharType, false, true, OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH},
};

bool is_ivf_user_row_aux_type(const ObLogicalTableAuxType aux_type)
{
  return LT_AUX_IVF_CID_ROWID == aux_type || LT_AUX_IVF_ROWID_CID == aux_type;
}

int get_ivf_cid_key_suffix_ordinal(const ObLogicalTableAuxType aux_type,
                                   int64_t &cid_key_ordinal)
{
  int ret = OB_SUCCESS;
  cid_key_ordinal = common::OB_INVALID_INDEX;
  if (LT_AUX_IVF_CID_ROWID == aux_type) {
    cid_key_ordinal = OB_LOGICAL_IVF_CID_ROWID_CID_ORDINAL;
  } else if (LT_AUX_IVF_ROWID_CID == aux_type) {
    cid_key_ordinal = OB_LOGICAL_IVF_ROWID_CID_CID_ORDINAL;
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

// O(1) lookup table indexed by aux_type
struct AuxTypeColDescEntry
{
  const ObLogicalTableKeyColDesc *col_descs_;
  int64_t col_cnt_;
};

static const AuxTypeColDescEntry AUX_TYPE_COL_DESC_TABLE[] = {
  /* LT_AUX_PRIMARY        */ {LT_AUX_PRIMARY_COLS,        ARRAYSIZEOF(LT_AUX_PRIMARY_COLS)},
  /* LT_AUX_SEARCH_INDEX   */ {LT_AUX_SEARCH_INDEX_COLS,   ARRAYSIZEOF(LT_AUX_SEARCH_INDEX_COLS)},
  /* LT_AUX_FTS_INVERTED   */ {LT_AUX_FTS_INVERTED_COLS,   ARRAYSIZEOF(LT_AUX_FTS_INVERTED_COLS)},
  /* LT_AUX_FTS_FORWARD    */ {LT_AUX_FTS_FORWARD_COLS,    ARRAYSIZEOF(LT_AUX_FTS_FORWARD_COLS)},
  /* LT_AUX_IVF_CID_VECTOR */ {LT_AUX_IVF_CID_VECTOR_COLS, ARRAYSIZEOF(LT_AUX_IVF_CID_VECTOR_COLS)},
  /* LT_AUX_IVF_CID_ROWID  */ {LT_AUX_IVF_CID_ROWID_COLS,  ARRAYSIZEOF(LT_AUX_IVF_CID_ROWID_COLS)},
  /* LT_AUX_IVF_ROWID_CID  */ {LT_AUX_IVF_ROWID_CID_COLS,  ARRAYSIZEOF(LT_AUX_IVF_ROWID_CID_COLS)},
};

// Compile-time check: table size must match enum count
STATIC_ASSERT(ARRAYSIZEOF(AUX_TYPE_COL_DESC_TABLE) == LT_AUX_MAX_TYPE,
              "AUX_TYPE_COL_DESC_TABLE size mismatch with LT_AUX_MAX_TYPE");

int get_aux_type_col_descs(
  const ObLogicalTableAuxType aux_type,
  const ObLogicalTableKeyColDesc *&col_descs,
  int64_t &col_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(aux_type >= LT_AUX_MAX_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid aux_type", K(ret), K(aux_type));
  } else {
    col_descs = AUX_TYPE_COL_DESC_TABLE[aux_type].col_descs_;
    col_cnt = AUX_TYPE_COL_DESC_TABLE[aux_type].col_cnt_;
  }
  return ret;
}

ObLogicalTableAuxType classify_aux_type_from_schema(const schema::ObTableSchema &table_schema)
{
  ObLogicalTableAuxType aux_type = LT_AUX_PRIMARY;
  if (table_schema.is_fts_index_aux()) {
    aux_type = LT_AUX_FTS_INVERTED;
  } else if (table_schema.is_fts_doc_word_aux()) {
    aux_type = LT_AUX_FTS_FORWARD;
  } else if (table_schema.is_search_index()) {
    aux_type = LT_AUX_SEARCH_INDEX;
  } else if (table_schema.is_vec_ivf_centroid_index()) {
    aux_type = LT_AUX_IVF_CID_VECTOR;
  } else if (table_schema.is_vec_ivfflat_cid_vector_index()
             || table_schema.is_vec_ivfsq8_cid_vector_index()
             || table_schema.is_vec_ivfpq_code_index()) {
    aux_type = LT_AUX_IVF_CID_ROWID;
  } else if (table_schema.is_vec_ivfflat_rowkey_cid_index()
             || table_schema.is_vec_ivfsq8_rowkey_cid_index()
             || table_schema.is_vec_ivfpq_rowkey_cid_index()) {
    aux_type = LT_AUX_IVF_ROWID_CID;
  } else if (table_schema.is_index_table()) {
    // Unrecognized index schema: legacy DML fallback. Keep parity with
    // ObDelUpdLogPlan::classify_logical_aux_type_'s historical default
    // so DML behavior remains unchanged after the merge.
    aux_type = LT_AUX_SEARCH_INDEX;
  } else {
    // Non-index schema (primary user table, etc.)
    aux_type = LT_AUX_PRIMARY;
  }
  return aux_type;
}

bool is_logical_rowid_column_name(const common::ObString &column_name)
{
  return !column_name.empty()
      && 0 == column_name.case_compare(OB_HIDDEN_PK_INCREMENT_COLUMN_NAME);
}

bool extract_collection_id_from_kv_table_name(const common::ObString &table_name,
                                              common::ObString &collection_id)
{
  static const common::ObString suffix = common::ObString::make_string("_kv_data_table");
  bool extracted = false;
  common::ObString tmp_collection_id;
  if (table_name.length() > suffix.length() && table_name.suffix_match(suffix.ptr())) {
    tmp_collection_id.assign_ptr(table_name.ptr(), table_name.length() - suffix.length());
    collection_id = tmp_collection_id;
    extracted = true;
  }
  return extracted;
}

bool is_logical_namespace_column_name(const common::ObString &column_name)
{
  return !column_name.empty()
      && 0 == column_name.case_compare("namespace_id");
}

bool is_logical_ltable_column_name(const common::ObString &column_name)
{
  return !column_name.empty()
      && 0 == column_name.case_compare("ltable_id");
}

bool is_logical_namespace_column(const sql::ObColumnRefRawExpr *col)
{
  return OB_NOT_NULL(col) && is_logical_namespace_column_name(col->get_column_name());
}

bool is_logical_ltable_column(const sql::ObColumnRefRawExpr *col)
{
  return OB_NOT_NULL(col) && is_logical_ltable_column_name(col->get_column_name());
}

bool is_logical_table_literal_or_param_expr(const sql::ObRawExpr *expr)
{
  return OB_NOT_NULL(expr)
      && expr->is_const_raw_expr()
      && IS_DATATYPE_OR_QUESTIONMARK_OP(expr->get_expr_type());
}

bool extract_logical_ns_const_value(const sql::ObRawExpr *expr,
                                    int64_t &out_param_idx,
                                    uint64_t &out_literal)
{
  bool extracted = false;
  int64_t tmp_param_idx = -1;
  uint64_t tmp_literal = 0;
  if (OB_NOT_NULL(expr) && expr->is_const_raw_expr()) {
    const sql::ObConstRawExpr *const_expr = static_cast<const sql::ObConstRawExpr *>(expr);
    const common::ObObj &value = const_expr->get_value();
    if (value.is_unknown()) {
      extracted = (OB_SUCCESS == value.get_unknown(tmp_param_idx) && tmp_param_idx >= 0);
    } else if (OB_SUCCESS == value.get_uint64(tmp_literal)) {
      extracted = true;
    } else {
      int64_t tmp_int = 0;
      if (OB_SUCCESS == value.get_int(tmp_int) && tmp_int >= 0) {
        tmp_literal = static_cast<uint64_t>(tmp_int);
        extracted = true;
      }
    }
  }
  if (extracted) {
    out_param_idx = tmp_param_idx;
    out_literal = tmp_literal;
  }
  return extracted;
}

bool record_logical_ns_if_compatible(const int64_t param_idx,
                                     const uint64_t literal,
                                     int64_t &recorded_param_idx,
                                     uint64_t &recorded_literal)
{
  const bool unset = recorded_param_idx < 0 && 0 == recorded_literal;
  const bool same = param_idx >= 0
      ? recorded_param_idx == param_idx
      : recorded_param_idx < 0 && recorded_literal == literal;
  if (unset) {
    if (param_idx >= 0) {
      recorded_param_idx = param_idx;
      recorded_literal = 0;
    } else {
      recorded_param_idx = -1;
      recorded_literal = literal;
    }
  }
  return unset || same;
}

bool is_same_logical_table_const_expr(const sql::ObRawExpr *lhs, const sql::ObRawExpr *rhs)
{
  bool same = false;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
  } else if (!is_logical_table_literal_or_param_expr(lhs)
             || !is_logical_table_literal_or_param_expr(rhs)) {
  } else if (T_QUESTIONMARK == lhs->get_expr_type()
             || T_QUESTIONMARK == rhs->get_expr_type()) {
    // A '?' param on either side: after parameterization the same literal may
    // be bound to different param indices, so index equality is not a reliable
    // signal. Do not reject here; the runtime check at
    // ObTableInsertOp::check_logical_table_row_ns_lt_consistency() compares
    // actual values.
    same = true;
  } else if (lhs->is_const_raw_expr() && rhs->is_const_raw_expr()) {
    // Both sides are plain literals: compare actual values and reject clearly
    // inconsistent data.
    const sql::ObConstRawExpr *lhs_c = static_cast<const sql::ObConstRawExpr *>(lhs);
    const sql::ObConstRawExpr *rhs_c = static_cast<const sql::ObConstRawExpr *>(rhs);
    same = (0 == lhs_c->get_value().compare(rhs_c->get_value()));
  }
  return same;
}

int extract_logical_table_eq_pred_from_expr(
  const sql::ObRawExpr *raw_expr,
  const uint64_t loc_table_id,
  const uint64_t alias_table_id,
  sql::ObRawExpr *&ns_expr,
  sql::ObRawExpr *&lt_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
  } else if (raw_expr->is_op_expr()) {
    const sql::ObOpRawExpr *op_expr = static_cast<const sql::ObOpRawExpr *>(raw_expr);
    const ObItemType op_type = op_expr->get_expr_type();
    if (T_OP_AND == op_type) {
      for (int64_t i = 0; OB_SUCC(ret) && i < op_expr->get_param_count(); ++i) {
        if (OB_FAIL(extract_logical_table_eq_pred_from_expr(op_expr->get_param_expr(i),
                                                            loc_table_id, alias_table_id,
                                                            ns_expr, lt_expr))) {
          LOG_WARN("failed to extract logical table eq pred", K(ret));
        }
      }
    } else if (T_OP_EQ == op_type || T_OP_NSEQ == op_type) {
      const sql::ObRawExpr *lhs = op_expr->get_param_expr(0);
      const sql::ObRawExpr *rhs = op_expr->get_param_expr(1);
      if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
      } else if (!lhs->is_column_ref_expr()) {
      } else {
        const sql::ObColumnRefRawExpr *col = static_cast<const sql::ObColumnRefRawExpr *>(lhs);
        if (col->get_table_id() != loc_table_id && col->get_table_id() != alias_table_id) {
        } else if ((is_logical_namespace_column(col) || is_logical_ltable_column(col))
                   && !is_logical_table_literal_or_param_expr(rhs)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "namespace_id/ltable_id/rowid predicate must be constant");
          LOG_WARN("logical table DML predicate RHS must be const expr", K(ret), KPC(rhs));
        } else if (is_logical_namespace_column(col)) {
          if (OB_NOT_NULL(ns_expr)) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple namespace_id predicates in WHERE");
            LOG_WARN("duplicate namespace_id predicate", K(ret));
          } else {
            ns_expr = const_cast<sql::ObRawExpr *>(rhs);
          }
        } else if (is_logical_ltable_column(col)) {
          if (OB_NOT_NULL(lt_expr)) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple ltable_id predicates in WHERE");
            LOG_WARN("duplicate ltable_id predicate", K(ret));
          } else {
            lt_expr = const_cast<sql::ObRawExpr *>(rhs);
          }
        }
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObLogicalTablePlanCtx,
  ns_param_idx_,
  ns_literal_,
  dml_ns_param_idx_,
  dml_ns_literal_,
  collection_id_);

} // namespace share
} // namespace oceanbase
