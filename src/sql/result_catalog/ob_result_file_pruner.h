/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESULT_CATALOG_OB_RESULT_FILE_PRUNER_H_
#define OCEANBASE_SQL_RESULT_CATALOG_OB_RESULT_FILE_PRUNER_H_

#include "sql/optimizer/file_prune/ob_i_lake_table_file_pruner.h"

namespace oceanbase
{
namespace sql
{

// RESULT_SCAN already has the exact immutable file manifest in its TableItem.
// This pruner only supplies the generic lake-table location identity required
// by ObLakeTablePartitionInfo; it never reloads metadata or performs catalog
// cache lookups.
class ObResultFilePruner final : public ObILakeTableFilePruner
{
public:
  explicit ObResultFilePruner(common::ObIAllocator &allocator);
  ~ObResultFilePruner() override = default;

  int init(const uint64_t table_id, const uint64_t ref_table_id);
  int assign(const ObILakeTableFilePruner &other) override;
  int clone(common::ObIAllocator &allocator,
            ObILakeTableFilePruner *&pruner) const override;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESULT_CATALOG_OB_RESULT_FILE_PRUNER_H_
