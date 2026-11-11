/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_SQL_DAS_SEARCH_INDEX_TEXT_INFO_H_
#define OCEANBASE_SQL_DAS_SEARCH_INDEX_TEXT_INFO_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace sql
{
class ObColumnRefRawExpr;
struct ObSearchIndexTextInfo;

// Execution metadata assembled from a source field and its Search Text info.
// All strings belong to the plan/DAS allocator.
struct ObDASSearchIndexTextInfo
{
  OB_UNIS_VERSION(1);
public:
  explicit ObDASSearchIndexTextInfo(common::ObIAllocator *allocator = nullptr);
  bool has_search_index_fulltext() const { return column_idx_ >= 0; }
  // The info must be a successfully resolved Search FULLTEXT binding for the field.
  int assign(const ObColumnRefRawExpr &field, const ObSearchIndexTextInfo &info,
             common::ObIAllocator &allocator);

  int64_t column_idx_;
  common::ObString encoded_path_;
  common::ObString parser_name_;
  common::ObString parser_properties_;
  bool is_json_; // JSON tokens retain JSON VALUE encoding; SQL strings use SQL VALUE encoding.

  TO_STRING_KV(K_(column_idx), K_(encoded_path), K_(parser_name), K_(parser_properties), K_(is_json));
private:
  int deep_copy_strings();
  common::ObIAllocator *allocator_; // Borrowed plan/DAS allocator for string deep copies.
};
} // namespace sql
} // namespace oceanbase
#endif
