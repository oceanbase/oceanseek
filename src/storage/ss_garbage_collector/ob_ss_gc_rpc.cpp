/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/ob_ss_gc_rpc.h"
#include "share/ob_cluster_version.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/worker.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
OB_SERIALIZE_MEMBER(ObCheckAtomicProtocolArg, tenant_id_, trace_id_);
}  // namespace storage

namespace obrpc
{
int ObCheckAtomicProtocolP::process()
{
  lib::ErrsimModuleGuard errsim_guard(common::ObErrsimModuleType::ERRSIM_MODULE_SS_GC);
  int ret = OB_SUCCESS;
  const storage::ObCheckAtomicProtocolArg &arg = arg_;
  bool dag_net_exist = false;
  bool dag_exist = false;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else {
    MTL_SWITCH(arg.tenant_id_) {
      share::ObTenantDagScheduler *dag_scheduler = nullptr;
      const int64_t abs_timeout_us = THIS_WORKER.get_timeout_ts();
      uint64_t tenant_data_version = 0;
      if (OB_ISNULL(dag_scheduler = MTL(share::ObTenantDagScheduler *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, dag scheduler must not be nullptr", KR(ret), K(arg));
      } else if (OB_FAIL(dag_scheduler->check_dag_net_exist(
          arg.trace_id_, dag_net_exist, abs_timeout_us))) {
        LOG_WARN("failed to check dag net exist", KR(ret), K(arg));
      } else if (dag_net_exist) {
        result_ = true;
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(arg.tenant_id_, tenant_data_version))) {
        LOG_WARN("failed to get tenant data version", KR(ret), K(arg));
      } else if (tenant_data_version < DATA_VERSION_4_6_2_1) {
        // Older data versions cannot check ordinary DAG liveness. Treat the
        // result as existing conservatively to prevent fail GC from abandoning
        // an atomic protocol that may still have a live owner.
        result_ = true;
      } else if (OB_FAIL(dag_scheduler->check_dag_exist(
          arg.trace_id_, dag_exist, abs_timeout_us))) {
        LOG_WARN("failed to check dag exist", KR(ret), K(arg), K(tenant_data_version));
      } else {
        result_ = dag_exist;
      }
    } else {
      if (OB_TENANT_NOT_IN_SERVER == ret) {
        // Tenant not on this server: treat as not-exist rather than an RPC failure.
        ret = OB_SUCCESS;
        result_ = false;
      } else {
        LOG_WARN("failed to switch tenant", KR(ret), K(arg));
      }
    }
  }
  return ret;
}
}  // namespace obrpc
}  // namespace oceanbase
