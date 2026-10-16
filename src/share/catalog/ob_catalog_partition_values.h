/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_CATALOG_OB_CATALOG_PARTITION_VALUES_H_
#define OCEANBASE_SHARE_CATALOG_OB_CATALOG_PARTITION_VALUES_H_

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

struct CatalogPartitionValues
{
  CatalogPartitionValues() : partition_name_(), subpartition_name_()
  {
  }
  explicit CatalogPartitionValues(const common::ObString &partition_name)
      : partition_name_(partition_name), subpartition_name_()
  {
  }
  CatalogPartitionValues(const common::ObString &partition_name,
                         const common::ObString &subpartition_name)
      : partition_name_(partition_name), subpartition_name_(subpartition_name)
  {
  }

  bool is_subpartition() const
  {
    return !subpartition_name_.empty();
  }

  TO_STRING_KV(K_(partition_name), K_(subpartition_name));

  common::ObString partition_name_;
  common::ObString subpartition_name_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_OB_CATALOG_PARTITION_VALUES_H_
