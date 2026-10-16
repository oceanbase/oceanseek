/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX SHARE

#include "share/catalog/hive/ob_hive_metastore_util.h"

#include "share/catalog/hive/ob_hive_metastore.h"

namespace oceanbase
{
namespace share
{
int ObHiveMetastoreUtils::get_warehouse_dir(ObHiveMetastoreClient &client,
                                            common::ObIAllocator &allocator,
                                            common::ObString &warehouse_dir)
{
  int ret = OB_SUCCESS;
  ObString default_warehouse = ObString::make_string("/user/hive/warehouse");
  ObString key = ObString::make_string("hive.metastore.warehouse.dir");
  if (OB_FAIL(client.get_config_value(key, default_warehouse, allocator, warehouse_dir))) {
    LOG_WARN("failed to get warehouse dir, using default", K(ret));
    ret = OB_SUCCESS;
    if (OB_FAIL(ob_write_string(allocator, default_warehouse, warehouse_dir))) {
      LOG_WARN("failed to write warehouse dir", K(ret));
    }
  }
  return ret;
}
} // namespace share
} // namespace oceanbase
