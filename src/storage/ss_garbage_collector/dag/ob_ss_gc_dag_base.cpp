/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_base.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

ObSSGCDagBase::ObSSGCDagBase(const ObDagType::ObDagTypeEnum dag_type)
  : ObIDag(dag_type)
{
}

int ObSSGCDagBase::get_published_dag_ret() const
{
  return ATOMIC_LOAD(&dag_ret_);
}

bool ObSSGCDagBase::is_stop_requested()
{
  lib::ObMutexGuard guard(lock_);
  return is_stop_;
}

int ObSSGCDagBase::check_if_stopped_or_failed(ObIDag &dag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_ss_gc_dag_type(dag.get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected shared storage gc dag type", KR(ret),
        "dag_type", dag.get_type(), KP(&dag));
  } else {
    ObSSGCDagBase &gc_dag = static_cast<ObSSGCDagBase &>(dag);
    if (gc_dag.is_stop_requested()) {
      ret = OB_CANCELED;
      LOG_INFO("shared storage gc dag has been canceled", KR(ret),
          "dag_type", dag.get_type(), KP(&dag));
    } else {
      const int dag_ret = gc_dag.get_published_dag_ret();
      if (OB_SUCCESS != dag_ret) {
        ret = OB_CANCELED;
        LOG_INFO("stop gc task because a sibling task failed", KR(ret),
            K(dag_ret), "dag_type", dag.get_type(), KP(&dag));
      }
    }
  }
  return ret;
}

bool ObSSGCDagBase::is_ss_gc_dag_type(
    const ObDagType::ObDagTypeEnum dag_type)
{
  return ObDagType::DAG_TYPE_MARK_SWEEP == dag_type || ObDagType::DAG_TYPE_SS_GC_EXEC == dag_type
      || ObDagType::DAG_TYPE_SS_FAIL_GC == dag_type || ObDagType::DAG_TYPE_SS_LOCK_ROW_GC == dag_type
      || ObDagType::DAG_TYPE_SS_LS_META_GC == dag_type || ObDagType::DAG_TYPE_SS_DATA_CHECK == dag_type;
}

} // namespace storage
} // namespace oceanbase
