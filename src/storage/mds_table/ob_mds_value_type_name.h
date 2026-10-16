/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_VALUE_TYPE_NAME_H_
#define OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_VALUE_TYPE_NAME_H_

#include <cstdint>

namespace oceanbase
{
namespace storage
{

/// Human-readable VALUE_TYPE string for \p mds_type (mds_register.h order).
/// Unknown ids yield "UNKNOWN_MDS_VALUE_TYPE".
const char *get_mds_value_type_name(const uint16_t mds_type);

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_VALUE_TYPE_NAME_H_
