/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_LOGICAL_BM25_SCAN_HOST_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_LOGICAL_BM25_SCAN_HOST_H_

#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace storage { class ObTableScanParam; }
namespace sql
{
struct ObDASBaseCtDef;
struct ObDASBaseRtDef;
class ObEvalCtx;
struct ObLogicalTableSearchCtx;

// Minimal host abstraction used by ObLogicalFilteredBM25StatsProvider so it can
// run under both hybrid_search (ObDASSearchCtx + ObDASScalarScanCtDef) and the
// plain logical-table MATCH path (ObDASTRMergeIter + ObDASScanCtDef).
class ObILogicalBm25ScanHost
{
public:
  virtual ~ObILogicalBm25ScanHost() = default;
  virtual ObEvalCtx *get_eval_ctx() = 0;
  virtual const ObLogicalTableSearchCtx &get_logical_table_ctx() const = 0;
  virtual int get_related_tablet_id(const ObDASBaseCtDef *ctdef,
                                    common::ObTabletID &tablet_id) = 0;
  virtual int init_scan_param(common::ObTabletID &tablet_id,
                              const ObDASBaseCtDef *ctdef,
                              ObDASBaseRtDef *rtdef,
                              storage::ObTableScanParam &scan_param) = 0;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_LOGICAL_BM25_SCAN_HOST_H_
