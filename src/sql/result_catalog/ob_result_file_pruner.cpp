/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT

#include "sql/result_catalog/ob_result_file_pruner.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace sql
{

ObResultFilePruner::ObResultFilePruner(common::ObIAllocator &allocator)
  : ObILakeTableFilePruner(allocator)
{
}

int ObResultFilePruner::init(const uint64_t table_id, const uint64_t ref_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Result Catalog file pruner is already initialized", KR(ret));
  } else {
    loc_meta_.table_loc_id_ = table_id;
    loc_meta_.ref_table_id_ = ref_table_id;
    loc_meta_.route_policy_ = READONLY_ZONE_FIRST;
    loc_meta_.is_external_table_ = true;
    loc_meta_.is_lake_table_ = true;
    loc_meta_.is_external_files_on_disk_ = false;
    is_partitioned_ = false;
    need_all_ = true;
    all_partitions_selected_ = true;
    inited_ = true;
  }
  return ret;
}

int ObResultFilePruner::assign(const ObILakeTableFilePruner &other)
{
  int ret = OB_SUCCESS;
  const ObResultFilePruner &result_pruner =
      static_cast<const ObResultFilePruner &>(other);
  if (OB_FAIL(ObILakeTableFilePruner::assign(result_pruner))) {
    LOG_WARN("failed to assign Result Catalog file pruner", KR(ret));
  }
  return ret;
}

int ObResultFilePruner::clone(common::ObIAllocator &allocator,
                               ObILakeTableFilePruner *&pruner) const
{
  int ret = OB_SUCCESS;
  pruner = nullptr;
  ObResultFilePruner *result_pruner = nullptr;
  if (OB_ISNULL(result_pruner = OB_NEWx(ObResultFilePruner, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate Result Catalog file pruner", KR(ret));
  } else if (OB_FAIL(result_pruner->assign(*this))) {
    LOG_WARN("failed to clone Result Catalog file pruner", KR(ret));
  } else {
    pruner = result_pruner;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
