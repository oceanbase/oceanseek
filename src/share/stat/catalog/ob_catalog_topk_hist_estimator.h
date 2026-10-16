/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_CATALOG_TOPK_HIST_ESTIMATOR_H
#define OB_CATALOG_TOPK_HIST_ESTIMATOR_H

#include "share/stat/catalog/ob_catalog_stats_estimator.h"

namespace oceanbase
{
namespace common
{

/**
 * @brief ObCatalogTopkHistEstimator
 * TopK frequency histogram estimator for catalog (external) tables.
 * Mirrors internal table ObTopkHistEstimator; not implemented yet.
 */
class ObCatalogTopkHistEstimator : public ObCatalogStatsEstimator
{
public:
  explicit ObCatalogTopkHistEstimator(sql::ObExecContext &ctx, ObIAllocator &allocator);

  int estimate(const ObOptCatalogStatGatherParam &param,
               ObOptCatalogStat &opt_stat);

private:
  int fill_hints(common::ObIAllocator &alloc,
                 const ObString &table_name,
                 int64_t gather_vectorize);

  int add_topk_hist_stat_items(
      const ObIArray<ObCatalogColumnStatParam> &column_params,
      ObOptCatalogStat &opt_stat);
};

} // namespace common
} // namespace oceanbase

#endif // OB_CATALOG_TOPK_HIST_ESTIMATOR_H
