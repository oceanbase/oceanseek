/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_search_index_text_info.h"
#include "sql/hybrid_search/ob_fulltext_search_node.h"
#include "sql/rewrite/ob_search_index_query_range_utils.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "lib/json_type/ob_json_path.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace share;
using namespace share::schema;

ObDASSearchIndexTextInfo::ObDASSearchIndexTextInfo(ObIAllocator *allocator)
  : column_idx_(-1), encoded_path_(), parser_name_(), parser_properties_(), is_json_(false), allocator_(allocator)
{}

int ObDASSearchIndexTextInfo::assign(
    const ObColumnRefRawExpr &field, const ObSearchIndexTextInfo &info, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  *this = ObDASSearchIndexTextInfo(&allocator);
  ObJsonPath parsed(info.path_, &allocator);
  parsed.is_mysql_ = true;
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> items;
  ObString encoded_path;
  bool path_supported = false;
  if (OB_FAIL(parsed.parse_path())) {
    if (ret != OB_ALLOCATE_MEMORY_FAILED) {
      ret = OB_INVALID_ARGUMENT;
    }
    LOG_WARN("failed to parse Search Text execution path", K(ret), K(info.path_));
  } else if (OB_FAIL(ObSearchIndexQueryRangeUtils::build_json_path_items(
      allocator, parsed, items, path_supported))) {
    LOG_WARN("failed to build Search Text path items", K(ret), K(info.path_));
  } else if (OB_FAIL(ObSearchIndexPathEncoder::encode_text_path(allocator, items, encoded_path))) {
    LOG_WARN("failed to encode Search Text execution path", K(ret), K(info.path_));
  } else if (OB_FAIL(ob_write_string(allocator, info.parser_name_, parser_name_))) {
    LOG_WARN("failed to copy Search Text parser name", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, info.parser_properties_, parser_properties_))) {
    LOG_WARN("failed to copy Search Text parser properties", K(ret));
  } else {
    column_idx_ = info.column_idx_;
    encoded_path_ = encoded_path;
    is_json_ = field.get_result_type().is_json();
  }
  if (OB_FAIL(ret)) {
    *this = ObDASSearchIndexTextInfo(&allocator);
  }
  return ret;
}

int ObDASSearchIndexTextInfo::deep_copy_strings()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("missing allocator for Search Text execution strings", K(ret));
  } else {
    ObString *strings[] = {&encoded_path_, &parser_name_, &parser_properties_};
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(strings); ++i) {
      ObString copied;
      if (OB_FAIL(ob_write_string(*allocator_, *strings[i], copied))) {
        LOG_WARN("failed to copy Search Text execution string", K(ret), K(i));
      } else {
        *strings[i] = copied;
      }
    }
  }
  return ret;
}

#define SEARCH_TEXT_INFO_FIELDS \
    column_idx_, encoded_path_, parser_name_, parser_properties_, is_json_

OB_DEF_SERIALIZE(ObDASSearchIndexTextInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, SEARCH_TEXT_INFO_FIELDS);
  return ret;
}

OB_DEF_DESERIALIZE(ObDASSearchIndexTextInfo)
{
  int ret = OB_SUCCESS;
  *this = ObDASSearchIndexTextInfo(allocator_);
  LST_DO_CODE(OB_UNIS_DECODE, SEARCH_TEXT_INFO_FIELDS);
  if (OB_SUCC(ret) && has_search_index_fulltext() && OB_FAIL(deep_copy_strings())) {
    LOG_WARN("failed to own deserialized Search Text execution strings", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASSearchIndexTextInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, SEARCH_TEXT_INFO_FIELDS);
  return len;
}
#undef SEARCH_TEXT_INFO_FIELDS
} // namespace sql
} // namespace oceanbase
