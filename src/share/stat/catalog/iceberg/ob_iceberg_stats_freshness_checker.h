/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_ICEBERG_STATS_FRESHNESS_CHECKER_H
#define OB_ICEBERG_STATS_FRESHNESS_CHECKER_H

#include "share/stat/catalog/ob_catalog_stats_freshness_checker.h"

namespace oceanbase
{
namespace common
{

// Iceberg only uses schema_version for partition change detection
class ObIcebergStatsFreshnessChecker : public ObICatalogStatsFreshnessChecker
{
public:
  ObIcebergStatsFreshnessChecker()
  {
  }
  virtual ~ObIcebergStatsFreshnessChecker()
  {
  }

  virtual bool check_table_schema_changed(
      const ObCatalogExtPartitionInfo &new_part,
      const share::ObOptCatalogTableStat *existing_stat) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIcebergStatsFreshnessChecker);
};

} // namespace common
} // namespace oceanbase

#endif // OB_ICEBERG_STATS_FRESHNESS_CHECKER_H
