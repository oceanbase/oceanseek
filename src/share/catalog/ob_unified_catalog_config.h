/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_CONFIG_H_
#define OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_CONFIG_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace share
{
class ObUnifiedCatalogConfig
{
public:
  ObUnifiedCatalogConfig()
      : endpoint_(), realm_()
  {}
  int load(const uint64_t tenant_id, common::ObIAllocator &allocator);
  int parse(const common::ObString &value, common::ObIAllocator &allocator);

  static constexpr int64_t DEFAULT_CONNECT_TIMEOUT_MS = 3000;
  static constexpr int64_t DEFAULT_REQUEST_TIMEOUT_MS = 10000;
  common::ObString endpoint_;
  common::ObString realm_;

private:
  static int validate_endpoint(const common::ObString &endpoint);
  static constexpr int64_t MAX_REALM_LENGTH = 256;
  static constexpr const char *HTTP_PREFIX = "http://";
  static constexpr const char *HTTPS_PREFIX = "https://";
};
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_CONFIG_H_
