/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_HYBRID_SEARCH_DSL_PARAMETERIZER_
#define OCEANBASE_SQL_HYBRID_SEARCH_DSL_PARAMETERIZER_

#include "sql/hybrid_search/ob_hybrid_search_dsl_param.h"
#include "sql/plan_cache/ob_plan_cache_util.h"

namespace oceanbase
{
namespace common
{
class ObJsonNode;
}
namespace sql
{
class ObHybridSearchDslParameterizer
{
public:
  // Share JSON parsing with the full resolver; defer field lookup and conversion.
  static int parse_json(common::ObIAllocator &allocator, const common::ObString &text,
      common::ObJsonNode *&root, const char *&syntax_error,
      uint64_t &error_offset);
  // Text is decoded SQL input or a variable snapshot. Results use the request allocator.
  static int extract(common::ObIAllocator &allocator, const common::ObString &text,
      const ObHybridDslSource &source, ObHybridDslRequestInfo *&request,
      common::ObIArray<ObPCParam *> &raw_params);
  // The DSL carrier is already embedded in the SQL key and needs no substitution.
  static int remove_carrier_param(const ObHybridDslRequestInfo &request,
      const common::ObIArray<ObPCParam *> &raw_params,
      common::ObIArray<ObPCParam *> &special_params);
  // Preserve input types and decoded string bytes, independently of column schema.
  static int create_input_node(common::ObIAllocator &allocator,
      const common::ObIJsonBase &node, ParseNode *&literal);
private:
  class TemplateBuilder;
};
} // namespace sql
} // namespace oceanbase
#endif
