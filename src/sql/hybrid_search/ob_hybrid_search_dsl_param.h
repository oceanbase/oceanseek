/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// Request syntax used by hybrid-search fast parameterization.

#ifndef OCEANBASE_SQL_HYBRID_SEARCH_OB_HYBRID_SEARCH_DSL_PARAM_
#define OCEANBASE_SQL_HYBRID_SEARCH_OB_HYBRID_SEARCH_DSL_PARAM_

#include "lib/utility/utility.h"
#include "lib/container/ob_array_helper.h"
#include "lib/container/ob_bit_set.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "sql/ob_sql_define.h"
#include "sql/parser/parse_node.h"

namespace oceanbase
{
namespace common
{
class ObIJsonBase;
}

namespace sql
{
struct ObPCParam;
struct ObPlanCacheCtx;
// Source of the DSL carrier in SQL.
enum SourceKind
{
  SOURCE_INVALID = 0,
  SOURCE_LITERAL = 1,        // SQL string literal
  SOURCE_USER_VARIABLE = 2   // User variable read once during extraction
};

enum ObHybridDslFallbackReason
{
  DSL_FALLBACK_NONE = 0,
  DSL_FALLBACK_EXECUTABLE_COMMENT,
  DSL_FALLBACK_MULTIPLE_CARRIERS,
  DSL_FALLBACK_TABLE_SYNTAX,
  DSL_FALLBACK_CARRIER_SYNTAX,
  DSL_FALLBACK_INCOMPLETE_CALL,
  DSL_FALLBACK_FAST_PARSER_DISABLED,
  DSL_FALLBACK_BATCHED_MULTI_STMT,
  DSL_FALLBACK_USER_VARIABLE,
  DSL_FALLBACK_DSL_EXTRACTION,
  DSL_FALLBACK_SQL_REWRITE
};

// The DSL carrier in SQL. raw_param_idx_ indexes the outer raw literals,
// not the DSL parameters or the execution ParamStore.
struct ObHybridDslSource
{
  ObHybridDslSource()
      : present_(false), supported_(false), kind_(SOURCE_INVALID),
        fallback_reason_(DSL_FALLBACK_NONE), raw_param_idx_(-1),
        sql_offset_(-1), sql_length_(0), key_offset_(-1) {}
  bool present_;
  bool supported_;
  SourceKind kind_;
  ObHybridDslFallbackReason fallback_reason_;
  int64_t raw_param_idx_;
  int64_t sql_offset_;
  int64_t sql_length_;
  int64_t key_offset_;             // Offset in the first fast-parser SQL template.
  common::ObString literal_;       // SQL string decoding is already complete.
  common::ObString user_var_name_;
  TO_STRING_KV(K_(present), K_(supported), K_(kind), K_(fallback_reason), K_(raw_param_idx),
               K_(sql_offset), K_(sql_length), K_(key_offset), K_(user_var_name));
};

// Allocated only for HYBRID_SEARCH. All members use the request allocator.
// Query literals remain in ObFastParserResult::raw_params_; this object owns
// only syntax, source mapping and compilation state. Copies of the fast-parser
// result borrow this same object within one request.
class ObHybridDslRequestInfo
{
public:
  explicit ObHybridDslRequestInfo(common::ObIAllocator &allocator)
      : cacheable_(true), sql_raw_param_count_(-1), sql_param_count_(0),
        params_ready_(false), params_bound_(false), json_root_(nullptr),
        raw_params_(nullptr), param_store_(nullptr), carrier_node_(nullptr),
        param_nodes_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator)),
        resolved_param_idxs_(common::ModulePageAllocator(allocator))
  {}
  bool can_cache() const { return cacheable_; }
  bool has_parameters() const { return cacheable_ && sql_raw_param_count_ >= 0; }
  bool params_bound() const { return has_parameters() && params_bound_; }
  void reset_compile()
  {
    sql_param_count_ = 0;
    params_ready_ = false;
    params_bound_ = false;
    raw_params_ = nullptr;
    param_store_ = nullptr;
    carrier_node_ = nullptr;
    resolved_param_idxs_.reuse();
  }
  void set_cacheable(const bool cacheable) { cacheable_ = cacheable; }
  int64_t get_sql_raw_param_count() const { return sql_raw_param_count_; }
  void set_sql_raw_param_count(const int64_t count) { sql_raw_param_count_ = count; }
  int64_t get_sql_param_count() const { return sql_param_count_; }
  bool is_params_ready() const { return params_ready_; }
  void set_compile_params(common::ObIArray<ObPCParam *> &raw, ParamStore &params,
      const int64_t sql_param_count)
  {
    raw_params_ = &raw;
    param_store_ = &params;
    sql_param_count_ = sql_param_count;
    params_ready_ = true;
  }
  void set_carrier_node(const ParseNode *node) { carrier_node_ = node; }
  bool is_carrier_node(const ParseNode &node) const { return carrier_node_ == &node; }
  void set_params_bound(const bool is_bound) { params_bound_ = is_bound; }
  ObHybridDslSource &get_source() { return source_; }
  const ObHybridDslSource &get_source() const { return source_; }
  common::ObString &get_dsl_snapshot() { return dsl_snapshot_; }
  const common::ObString &get_dsl_snapshot() const { return dsl_snapshot_; }
  common::ObIJsonBase *get_json_root() const { return json_root_; }
  void set_json_root(common::ObIJsonBase *root) { json_root_ = root; }
  common::ObString &get_dsl_template() { return dsl_template_; }
  const common::ObString &get_dsl_template() const { return dsl_template_; }
  common::ObIArray<ObPCParam *> *get_raw_params() const { return raw_params_; }
  ParamStore *get_param_store() const { return param_store_; }
  int64_t get_dsl_param_count() const { return param_nodes_.count(); }
  // DSL-relative indices are the only indices accepted by node and bitmap accessors.
  int append_param_node(const common::ObIJsonBase &node);
  int find_dsl_param_idx(const common::ObIJsonBase &node, int64_t &dsl_idx) const;
  int check_param_layout(const common::ObIArray<ObPCParam *> &raw) const;
  int get_dsl_raw_param_idx(const int64_t dsl_idx,
      const common::ObIArray<ObPCParam *> &raw, int64_t &raw_idx) const;
  int get_dsl_exec_param_idx(const int64_t dsl_idx,
      const int64_t sql_param_count, int64_t &param_idx) const;
  int mark_param_resolved(const int64_t dsl_idx);
  const common::ObBitSet<> &get_resolved_param_idxs() const { return resolved_param_idxs_; }
  TO_STRING_KV(K_(source), KP_(json_root), K_(dsl_template));

private:
  int check_dsl_param_idx(const int64_t dsl_idx) const;
  int get_absolute_param_idx(const int64_t dsl_idx,
      const int64_t offset, int64_t &param_idx) const;
  bool cacheable_;
  int64_t sql_raw_param_count_; // R: outer raw literals, including the DSL carrier.
  int64_t sql_param_count_;     // N: ordinary execution parameters, excluding the carrier.
  bool params_ready_;
  bool params_bound_;
  ObHybridDslSource source_;
  common::ObString dsl_snapshot_;
  common::ObIJsonBase *json_root_;
  common::ObString dsl_template_;
  // Borrowed only during compilation; never copied into a cache value.
  common::ObIArray<ObPCParam *> *raw_params_;
  ParamStore *param_store_;
  // Only this full-parser node may consume the request snapshot and parameters.
  const ParseNode *carrier_node_;
  // Request-only mapping: param_nodes_[i] corresponds to raw_params_[R + i].
  common::ObSEArray<const common::ObIJsonBase *, 8> param_nodes_;
  common::ObBitSet<> resolved_param_idxs_; // DSL-relative expression binding progress.
};

} // namespace sql
} // namespace oceanbase
#endif
