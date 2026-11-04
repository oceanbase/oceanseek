/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_AI_DECISION_UTILS_H_
#define OCEANBASE_SQL_OB_AI_DECISION_UTILS_H_

#include "lib/json_type/ob_json_tree.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{

class ObAIDecisionUtils
{
public:
  static int get_header(ObIAllocator &allocator, const ObString &api_key,
                        ObArray<ObString> &headers);
  static bool is_openai_provider(const ObString &provider);
  static bool is_valid_url(const ObString &url);
  static int parse_output(ObIAllocator &allocator, ObJsonObject *response, ObJsonObject *&result);
private:
  ObAIDecisionUtils() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObAIDecisionUtils);
};

class ObTypeSafeDecisionUtils
{
public:
  static int get_body(ObIAllocator &allocator, const ObString &model,
                      ObJsonNode *state, ObJsonNode *questions, ObJsonObject *&body);
private:
  ObTypeSafeDecisionUtils() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObTypeSafeDecisionUtils);
};

class ObOpenAIDecisionUtils
{
public:
  static int get_body(ObIAllocator &allocator, const ObString &model,
                      ObJsonNode *input, ObJsonNode *questions, ObJsonObject *&body,
                      const ObJsonObject *parameters = nullptr);
private:
  ObOpenAIDecisionUtils() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObOpenAIDecisionUtils);
};

} // namespace common
} // namespace oceanbase
#endif
