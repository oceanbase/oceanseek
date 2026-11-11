/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_TABLE_FORMAT_UNIFIED_OB_UNIFIED_CATALOG_TABLE_CODEC_H_
#define OCEANBASE_SQL_TABLE_FORMAT_UNIFIED_OB_UNIFIED_CATALOG_TABLE_CODEC_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace sql
{

/** Decodes the Iceberg REST loadTable location used by UC queries. */
class ObUnifiedCatalogIcebergTableCodec
{
public:
  static int decode_metadata_location(common::ObIAllocator &allocator,
                                      const common::ObString &response_body,
                                      common::ObString &metadata_location);

private:
  static constexpr const char *METADATA_LOCATION_KEY = "metadata-location";
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_TABLE_FORMAT_UNIFIED_OB_UNIFIED_CATALOG_TABLE_CODEC_H_
