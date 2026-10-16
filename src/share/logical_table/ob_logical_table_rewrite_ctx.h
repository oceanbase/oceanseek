/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#pragma once

#include "sql/engine/expr/ob_expr.h"
#include "share/logical_table/ob_logical_table_define.h"  // OB_INVALID_NAMESPACE_ID etc.

namespace oceanbase
{
namespace sql
{

// Minimal POD scratch for ObLogicalTableHelper::rewrite_table_scan_param_key_ranges.
// Replaces the heavy ObLogicalTableHelper that was used purely as a scratch in the
// range-rewrite path. All fields are zero-initialized by value-init (= {}).
struct ObLogicalTableRewriteCtx
{
  // --- Input fields (caller fills before calling rewrite_table_scan_param_key_ranges) ---
  uint64_t namespace_id_;      // expr-evaluated ns_id from lt_rt
  uint64_t logical_table_id_;    // expr-evaluated lt_id from lt_rt

  // --- Output fields (written back by rewrite_table_scan_param_key_ranges) ---
  bool     is_enabled_;        // true iff rewrite succeeded and produced physical ranges
  ObExpr  *kv_key_expr_;       // pd_expr_spec.access_exprs_[0] of the scanned ctdef
  ObExpr  *kv_value_expr_;    // pd_expr_spec.access_exprs_[1] of the scanned ctdef
  // Note: namespace_id_ and logical_table_id_ above are ALSO updated as outputs.
  // physical_data_table_id and tablet_id are written directly into scan_param; not tracked here.
  TO_STRING_KV(K_(namespace_id), K_(logical_table_id), K_(is_enabled), KP_(kv_key_expr), KP_(kv_value_expr));
};

}  // namespace sql
}  // namespace oceanbase
