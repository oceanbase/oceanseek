/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SS_STORAGE_STAT_OPERATOR_H
#define OCEANBASE_SS_STORAGE_STAT_OPERATOR_H

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}

enum ReportType
{
  INVALID_TYPE = -1,
  PUBLIC_DATA,
  PUBLIC_META,
  PRIVATE_ALL,
  MAX_TYPE
};

namespace storage
{

class ObSSStorageStatTableOperator
{
public:
  ObSSStorageStatTableOperator(common::ObMySQLProxy &proxy)
    : proxy_(proxy) {}
  ~ObSSStorageStatTableOperator() {}
public:
  int update_tenant_space_usage(
      const ReportType type,
      const int64_t value);

private:
  common::ObMySQLProxy &proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSStorageStatTableOperator);
};
} // namespace storage
} // namespace oceanbase



# endif // OCEANBASE_SS_STORAGE_STAT_OPERATOR_H
