/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_HYBRID_SEARCH_PLAN_CACHE_
#define OCEANBASE_SQL_HYBRID_SEARCH_PLAN_CACHE_

#include "sql/hybrid_search/ob_hybrid_search_dsl_param.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{
struct FPContext;
struct ObFastParserResult;
struct ObRemoteSqlInfo;
struct SqlInfo;
class ObSQLSessionInfo;

// Recognizes the HYBRID_SEARCH carrier in tokens supplied by the SQL lexer.
class ObHybridSearchTokenHandler
{
public:
  ObHybridSearchTokenHandler()
      : state_(FIND_HYBRID), has_executable_comment_(false), session_(nullptr) {}
  void reset()
  {
    state_ = FIND_HYBRID;
    has_executable_comment_ = false;
    source_ = ObHybridDslSource();
  }
  void set_session(ObSQLSessionInfo *session) { session_ = session; }
  bool has_carrier() const { return source_.present_; }
  bool need_process_token(const char *token, const int64_t length, const bool is_normal) const
  {
    static const int64_t HYBRID_SEARCH_NAME_LENGTH = sizeof("hybrid_search") - 1;
    bool need_process = false;
    if (HYBRID_UNSUPPORTED == state_) {
      // Stop tracking tokens after fallback.
    } else if (FIND_HYBRID != state_ && HYBRID_COMPLETE != state_) {
      // Continue parsing the current HYBRID_SEARCH call.
      need_process = true;
    } else if (is_normal && length == HYBRID_SEARCH_NAME_LENGTH
               && ('h' == token[0] || 'H' == token[0])) {
      // Check identifiers that may be HYBRID_SEARCH.
      need_process = true;
    } else if (length >= 3 && '/' == token[0] && '*' == token[1] && '!' == token[2]) {
      // Track executable comments even before the first HYBRID_SEARCH identifier.
      need_process = true;
    }
    return need_process;
  }
  inline int finish_parse(common::ObIAllocator &allocator, const FPContext &context,
      ObFastParserResult &result);
  int process_token(const common::ObString &raw_token,
      const int64_t offset, const int64_t key_offset, const bool is_normal, const bool is_parameter,
      const bool is_ignored, const ParseNode *node, const int64_t raw_param_idx);
  int finish(common::ObIAllocator &allocator, ObFastParserResult &result) const;
private:
  int finish_parse_impl(common::ObIAllocator &allocator, const FPContext &context,
      ObFastParserResult &result);
  void reject_carrier(const ObHybridDslFallbackReason reason);
  enum State
  {
    FIND_HYBRID, EXPECT_LPAREN, EXPECT_TABLE, EXPECT_TABLE_NAME, IN_TABLE_NAME,
    EXPECT_DSL, EXPECT_RPAREN, HYBRID_COMPLETE, HYBRID_UNSUPPORTED
  };
  State state_;
  bool has_executable_comment_;
  ObHybridDslSource source_;
  ObSQLSessionInfo *session_;
};

inline int ObHybridSearchTokenHandler::finish_parse(common::ObIAllocator &allocator,
    const FPContext &context, ObFastParserResult &result)
{
  int ret = common::OB_SUCCESS;
  // Keep the ordinary SQL path inline without entering DSL finalization.
  if (has_carrier()) {
    ret = finish_parse_impl(allocator, context, result);
  }
  return ret;
}

// Hybrid-specific entry points used by SQL parameterization and the plan cache. Key lookup,
// ordinary parameter conversion, PlanSet matching and schema checks stay in SQL.
class ObHybridSearchPlanCache
{
public:
  // The caller keeps handler alive through fast_parser(), including its finish hook.
  static int init_fast_parser(ObHybridSearchTokenHandler &handler, ObSQLSessionInfo *session,
      const bool is_enabled, FPContext &fp_ctx);
  static int init_fast_parser(ObHybridSearchTokenHandler &handler, const ObPlanCacheCtx &ctx,
      FPContext &fp_ctx);
  static ObHybridDslRequestInfo *get_request(ObHybridDslRequestInfo *request);
  static ObHybridDslRequestInfo *get_request(const ObFastParserResult &result);
  static bool can_add_plan(const ObFastParserResult &result);
  // Prepare the hybrid-specific remote payload.
  static int prepare_remote_sql(ObPlanCacheCtx &ctx, ObRemoteSqlInfo &remote_sql_info);
  static int get_remote_sql(ObPlanCacheCtx &ctx, common::ObString &sql);

  static int finish_fast_parse(common::ObIAllocator &allocator, const FPContext &fp_ctx,
      ObFastParserResult &fp_result, ObSQLSessionInfo *session);
  // Hooks around the ordinary SQL parser/parameterization pipeline.
  static void prepare_parse(ObPlanCacheCtx &ctx, const common::ObString &sql,
      bool &add_plan, bool &transform_tree);
  static int transform_sql_params(common::ObIAllocator &allocator, ObPlanCacheCtx &ctx,
      ParseNode *tree, SqlInfo &sql_info, ParamStore &params,
      share::schema::ObMaxConcurrentParam::FixParamStore &fixed_params,
      const bool is_transform_outline);
  static int check_sql_param_info(ObPlanCacheCtx &ctx, const SqlInfo &sql_info,
      common::ObIArray<ObPCParam *> &special_params);
  static int append_dsl_params(ObPlanCacheCtx &ctx, SqlInfo &sql_info, ParamStore &params,
      common::ObIArray<ObPCParam *> &special_params);
  static int construct_sql(common::ObIAllocator &allocator, ObPlanCacheCtx &ctx,
      const SqlInfo &sql_info, const FPContext &fp_ctx,
      common::ObIArray<ObPCParam *> &special_params);
  static int check_raw_param_count(const ObFastParserResult &result, const int64_t normal_parse_count);
  static const char *fallback_reason_name(const ObHybridDslFallbackReason reason);
  static void log_fallback(const ObHybridDslRequestInfo &request, const int error);
private:
  static int prepare_dsl_key(common::ObIAllocator &allocator, const FPContext &fp_ctx,
      ObHybridDslRequestInfo &request, ObFastParserResult &result);
  static int normalize_carrier(ParseNode *tree, const ObHybridDslRequestInfo &request,
      int64_t &carrier_count, const ParseNode *&carrier_node);
};
} // namespace sql
} // namespace oceanbase
#endif
