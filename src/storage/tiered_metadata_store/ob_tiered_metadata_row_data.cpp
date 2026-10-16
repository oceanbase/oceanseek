/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/tiered_metadata_store/ob_tiered_metadata_row_data.h"

namespace oceanbase
{
namespace storage
{

OB_SERIALIZE_MEMBER(ObTieredMetaTableRowData, meta_type_, meta_value_);

bool ObTieredMetaTableRowData::is_valid() const
{
  // 从备份恢复出来的meta_type可能是无效的，这是预期内的，所以这里只检查 meta_value_。
  return !meta_value_.empty();
}

} // namespace storage
} // namespace oceanbase
