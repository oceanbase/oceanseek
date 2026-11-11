/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL
#include "sql/table_format/unified/ob_unified_catalog_table_codec.h"

#include "lib/allocator/ob_allocator.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObUnifiedCatalogIcebergTableCodec::decode_metadata_location(
    ObIAllocator &allocator,
    const ObString &response_body,
    ObString &metadata_location)
{
  int ret = OB_SUCCESS;
  ObJsonNode *root_node = NULL;
  metadata_location.reset();
  if (OB_FAIL(ObJsonParser::get_tree(&allocator, response_body, root_node))) {
    LOG_WARN("failed to parse UC Iceberg Table response", K(ret));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid UC Iceberg Table response", K(ret));
  } else {
    const ObJsonNode *location_node
        = static_cast<const ObJsonObject *>(root_node)->get_value(METADATA_LOCATION_KEY);
    if (OB_ISNULL(location_node) || ObJsonNodeType::J_STRING != location_node->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("UC Iceberg metadata location is missing", K(ret));
    } else {
      const ObString location = static_cast<const ObJsonString *>(location_node)->value();
      if (location.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("UC Iceberg metadata location is empty", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, location, metadata_location, true))) {
        LOG_WARN("failed to copy UC Iceberg metadata location", K(ret));
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
