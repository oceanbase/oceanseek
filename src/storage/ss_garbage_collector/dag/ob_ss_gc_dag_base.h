/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_GC_DAG_BASE_H_
#define OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_GC_DAG_BASE_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace storage
{

class ObSSGCDagBase : public share::ObIDag
{
public:
  explicit ObSSGCDagBase(const share::ObDagType::ObDagTypeEnum dag_type);
  virtual ~ObSSGCDagBase() = default;

  int get_published_dag_ret() const;
  bool is_stop_requested();
  static int check_if_stopped_or_failed(share::ObIDag &dag);
  static bool is_ss_gc_dag_type(
      const share::ObDagType::ObDagTypeEnum dag_type);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_GC_DAG_BASE_H_
