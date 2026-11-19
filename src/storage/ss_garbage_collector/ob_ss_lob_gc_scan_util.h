/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_SS_LOB_GC_SCAN_UTIL_H_
#define OCEANBASE_STORAGE_OB_SS_LOB_GC_SCAN_UTIL_H_

#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
struct ObColDesc;
}
}
namespace storage
{
class ObStorageSchema;

class ObSSLobGCScanUtil final
{
public:
  static constexpr int64_t CTRL_SCAN_COLUMN_COUNT = 3;

  // Return macro_id, hash and the hidden transaction version from the frozen schema.
  static int prepare_ctrl_scan_columns(const ObStorageSchema &storage_schema,
      common::ObIArray<share::schema::ObColDesc> &columns);
  // Use the fixed LOB Meta layout for info and lob_block_id; the iterator validates the schema.
  static int prepare_meta_scan_columns(common::ObIArray<share::schema::ObColDesc> &columns);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SS_LOB_GC_SCAN_UTIL_H_
