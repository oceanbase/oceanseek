/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_GC_DAG_UTIL_H_
#define OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_GC_DAG_UTIL_H_

#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObMySQLProxy;
class ObString;
template <typename T> class ObIArray;
}
namespace share
{
class ObLSID;
}
namespace storage
{

struct ObSSMetaCtrlKey;

class ObSSGCDagUtil final
{
public:
  // A sibling failure cancels the current task just like an explicit stop.
  static int check_if_stopped(const share::ObITask &task);
  // Enumerate LS entries represented by SHARED_LS_LOCK rows in the shared
  // meta control table, rather than every LS in the tenant LS service.
  static int get_all_ls_ids_from_meta_ctrl(
      const uint64_t tenant_id,
      common::ObMySQLProxy &sql_proxy,
      const int64_t sql_timeout_us,
      common::ObIArray<share::ObLSID> &ls_ids,
      const share::ObITask *task = nullptr);
  // The returned string uses memory allocated by allocator.
  static int serialize_meta_key(
      const ObSSMetaCtrlKey &ctrl_key,
      common::ObIAllocator &allocator,
      common::ObString &meta_key);
  // The returned string borrows its storage from allocator.
  static int serialize_prefix_meta_key(
      const ObSSMetaCtrlKey &ctrl_key,
      common::ObIAllocator &allocator,
      common::ObString &meta_key);

  template <typename DagType, typename DagCtxType>
  static int get_dag_ctx(
      const share::ObITask &task,
      DagCtxType *&ctx)
  {
    int ret = common::OB_SUCCESS;
    const share::ObDagType::ObDagTypeEnum expected_dag_type =
        DagType::DAG_TYPE;
    share::ObIDag *base_dag = task.get_dag();
    ctx = nullptr;
    if (OB_ISNULL(base_dag)
        || OB_UNLIKELY(expected_dag_type != base_dag->get_type())) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid shared storage gc dag", KR(ret),
          K(expected_dag_type), KP(base_dag));
    } else {
      DagType *dag = static_cast<DagType *>(base_dag);
      if (OB_ISNULL(ctx = dag->get_ctx()) || OB_UNLIKELY(!ctx->is_valid())) {
        ret = common::OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid shared storage gc dag context", KR(ret),
            K(expected_dag_type), KP(ctx));
        ctx = nullptr;
      }
    }
    return ret;
  }

private:
  ObSSGCDagUtil() = delete;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_GC_DAG_UTIL_H_
