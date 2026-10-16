/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_LOGICAL_TABLE_OB_NAMESPACE_RESOURCE_LIMIT_H_
#define OCEANBASE_SHARE_LOGICAL_TABLE_OB_NAMESPACE_RESOURCE_LIMIT_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace share
{

// Validate sdk_namespaces.info resource-limit fields.
// row_limit / size_limit: 0 = block writes, -1 = unlimited, >0 = threshold, other negatives invalid.
// rate_limit_enable: 0 or 1.
// qps/tps/data burst/refill: must be >= 0.
class ObNamespaceResourceLimitValidator
{
public:
  static int validate_row_limit(const int64_t value);
  static int validate_size_limit(const int64_t value);
  static int validate_rate_limit_enable(const int64_t value);
  static int validate_ru_rate(const int64_t value, const common::ObString &field_name);
  static int validate_patch_json(const common::ObString &json_str);
};

} // namespace share
} // namespace oceanbase

#endif /* OCEANBASE_SHARE_LOGICAL_TABLE_OB_NAMESPACE_RESOURCE_LIMIT_H_ */
