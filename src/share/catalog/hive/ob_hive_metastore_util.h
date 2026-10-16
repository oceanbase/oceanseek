/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef _SHARE_CATALOG_HIVE_OB_HIVE_METASTORE_UTIL_H
#define _SHARE_CATALOG_HIVE_OB_HIVE_METASTORE_UTIL_H

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace share
{
class ObHiveMetastoreClient;

class ObHiveMetastoreUtils
{
public:
  static int get_warehouse_dir(ObHiveMetastoreClient &client,
                               common::ObIAllocator &allocator,
                               common::ObString &warehouse_dir);
};

} // namespace share
} // namespace oceanbase

#endif // _SHARE_CATALOG_HIVE_OB_HIVE_METASTORE_UTIL_H
