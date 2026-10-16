/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_DEFINE_H_
#define OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_DEFINE_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "common/object/ob_obj_type.h"

namespace oceanbase {
namespace sql {
class ObColumnRefRawExpr;
class ObRawExpr;
}
namespace share {
namespace schema { class ObTableSchema; }

// Auxiliary table type enum, encoded into Key for self-describing encode/decode dispatch
enum ObLogicalTableAuxType : uint8_t
{
  LT_AUX_PRIMARY          = 0,   // primary table
  LT_AUX_SEARCH_INDEX     = 1,   // search index (scalar / JSON columns)
  LT_AUX_FTS_INVERTED     = 2,   // full-text inverted index
  LT_AUX_FTS_FORWARD      = 3,   // full-text forward index
  LT_AUX_IVF_CID_VECTOR   = 4,   // IVF cluster center table
  LT_AUX_IVF_CID_ROWID    = 5,   // IVF vector data table
  LT_AUX_IVF_ROWID_CID    = 6,   // IVF rowid-to-cid table
  LT_AUX_MAX_TYPE                 // sentinel for boundary check
};

// Logical IVF user-row auxiliary keys encode ObCenterId at a fixed width.
// Reuse the historical public constant instead of repeating a literal 16.
static constexpr int64_t OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH =
    common::OB_DOC_ID_COLUMN_BYTE_LENGTH;
static constexpr uint16_t OB_LOGICAL_TABLE_INVALID_VECTOR_SUBSCHEMA_ID = UINT16_MAX;
static constexpr int64_t OB_LOGICAL_IVF_CID_ROWID_CID_ORDINAL = 0;
static constexpr int64_t OB_LOGICAL_IVF_ROWID_CID_CID_ORDINAL = 1;

bool is_ivf_user_row_aux_type(const ObLogicalTableAuxType aux_type);
int get_ivf_cid_key_suffix_ordinal(const ObLogicalTableAuxType aux_type,
                                   int64_t &cid_key_ordinal);

// Key payload column descriptor (single column)
struct ObLogicalTableKeyColDesc
{
  common::ObObjType type_;
  bool is_nullable_;
  // True when the datum value is already order-preserving encoded.
  // Currently used by Search Index path/value so key encoding can append them directly.
  bool is_order_preserving_encoded_;
  // Fixed byte length for order-preserving varchar columns (0 means variable-length
  // via datum content). Used by decode_key_body to know how many bytes to read.
  int64_t fixed_length_;

  ObLogicalTableKeyColDesc()
    : type_(common::ObMaxType),
      is_nullable_(false),
      is_order_preserving_encoded_(false),
      fixed_length_(0)
  {}
  ObLogicalTableKeyColDesc(common::ObObjType type,
                            bool nullable,
                            bool order_preserving_encoded = false,
                            int64_t fixed_length = 0)
    : type_(type),
      is_nullable_(nullable),
      is_order_preserving_encoded_(order_preserving_encoded),
      fixed_length_(fixed_length)
  {}

  TO_STRING_KV(K_(type), K_(is_nullable), K_(is_order_preserving_encoded), K_(fixed_length));
};

// Helper: get col_descs and count by aux_type
int get_aux_type_col_descs(
  const ObLogicalTableAuxType aux_type,
  const ObLogicalTableKeyColDesc *&col_descs,
  int64_t &col_cnt);

// Single source of truth for "table schema -> ObLogicalTableAuxType" mapping.
// All callers (DML log plan, hybrid_search CG, future logical-table paths)
// MUST go through this function instead of inlining their own switch.
//
// Classification:
//   FTS index aux    (INDEX_TYPE_FTS_INDEX_*)        -> LT_AUX_FTS_INVERTED
//   FTS doc-word aux (INDEX_TYPE_FTS_DOC_WORD_*)     -> LT_AUX_FTS_FORWARD
//   search data index                                -> LT_AUX_SEARCH_INDEX
//   IVF centroid                                     -> LT_AUX_IVF_CID_VECTOR
//   IVF cid-vector (flat/sq8/pq code)                -> LT_AUX_IVF_CID_ROWID
//   IVF rowkey-cid (flat/sq8/pq)                     -> LT_AUX_IVF_ROWID_CID
//   other index schemas (unrecognized index)         -> LT_AUX_SEARCH_INDEX (legacy fallback)
//   non-index schema (primary user table, etc.)      -> LT_AUX_PRIMARY
ObLogicalTableAuxType classify_aux_type_from_schema(const schema::ObTableSchema &table_schema);

// True when column_name is the logical-table rowid column (__pk_increment).
// Single source of truth for CG/DAS logical rowid detection (TSC, hybrid search, FTS aux).
bool is_logical_rowid_column_name(const common::ObString &column_name);

// Physical KV tables use <collection_id>_kv_data_table. The monitor must still
// validate collection_id before using this non-owning suffix-stripped view as
// an internal-SQL identifier.
bool extract_collection_id_from_kv_table_name(const common::ObString &table_name,
                                              common::ObString &collection_id);

// True when column_name is namespace_id / ltable_id on a logical table.
bool is_logical_namespace_column_name(const common::ObString &column_name);
bool is_logical_ltable_column_name(const common::ObString &column_name);

// Column-ref overloads shared by TSC CG, DML resolver, and DML CG.
bool is_logical_namespace_column(const sql::ObColumnRefRawExpr *col);
bool is_logical_ltable_column(const sql::ObColumnRefRawExpr *col);

// Logical table ns/lt routing keys must be SQL literals or '?' parameters.
// ObRawExpr::is_const_expr() is intentionally too broad here: it also treats
// expressions such as MySQL user-variable reads as const expressions.
bool is_logical_table_literal_or_param_expr(const sql::ObRawExpr *expr);

// Extract a parameter index or literal without modifying outputs on failure.
// The caller strips column conversion and implicit casts.
bool extract_logical_ns_const_value(const sql::ObRawExpr *expr,
                                    int64_t &out_param_idx,
                                    uint64_t &out_literal);

// Record the first namespace parameter/literal and accept repeated equal
// values. Return false without mutation on conflict.
bool record_logical_ns_if_compatible(const int64_t param_idx,
                                     const uint64_t literal,
                                     int64_t &recorded_param_idx,
                                     uint64_t &recorded_literal);

// True when lhs and rhs are both logical-table literal/param exprs denoting the
// same identity. A '?' param on either side is treated as "same" (the runtime
// check ObTableInsertOp::check_logical_table_row_ns_lt_consistency() compares
// actual values).
bool is_same_logical_table_const_expr(const sql::ObRawExpr *lhs,
                                      const sql::ObRawExpr *rhs);

// Walk an AND/EQ predicate tree and collect the namespace_id / ltable_id RHS
// exprs bound to the given logical table id. Returns OB_NOT_SUPPORTED on a
// non-const RHS or duplicate ns/lt predicates.
int extract_logical_table_eq_pred_from_expr(const sql::ObRawExpr *raw_expr,
                                            const uint64_t loc_table_id,
                                            const uint64_t alias_table_id,
                                            sql::ObRawExpr *&ns_expr,
                                            sql::ObRawExpr *&lt_expr);

// Invalid ID constants
static const uint64_t OB_INVALID_NAMESPACE_ID = 0;
static const uint64_t OB_INVALID_LOGICAL_TABLE_ID = 0;

// 记录在 ObPhysicalPlan 上的逻辑表路由信息（scan / DML 两条通道 + collection_id）。
// 由 CG 阶段写入，PX admission / 远端 DAS 在求值时读取。
struct ObLogicalTablePlanCtx
{
  OB_UNIS_VERSION(1);
public:
  ObLogicalTablePlanCtx()
    : ns_param_idx_(-1),
      ns_literal_(0),
      dml_ns_param_idx_(-1),
      dml_ns_literal_(0),
      collection_id_()
  {}
  void reset()
  {
    ns_param_idx_ = -1;
    ns_literal_ = 0;
    dml_ns_param_idx_ = -1;
    dml_ns_literal_ = 0;
    collection_id_.reset();
  }
  TO_STRING_KV(K_(ns_param_idx), K_(ns_literal), K_(dml_ns_param_idx),
               K_(dml_ns_literal), K_(collection_id));

  // D5: PX admission 经 ParamStore 取 ns_id。参数化场景存 ParamStore 下标；
  // 未参数化字面量场景存字面量值；两者默认值表示"非逻辑表"。
  int64_t ns_param_idx_;      // 默认 -1（无参数化 ns 谓词）
  uint64_t ns_literal_;       // 默认 0（非逻辑表 / 无字面量 ns）
  // INSERT / REPLACE VALUES 的写入目标 namespace，与 scan 通道分离，
  // 避免 INSERT ... SELECT 误用源 namespace。
  int64_t dml_ns_param_idx_;  // 默认 -1（无参数化 DML ns）
  uint64_t dml_ns_literal_;   // 默认 0（非逻辑表 / 无字面量 DML ns）
  common::ObString collection_id_; // 默认空；setter 不做深拷
};

} // namespace share
} // namespace oceanbase
#endif /* OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_DEFINE_H_ */
