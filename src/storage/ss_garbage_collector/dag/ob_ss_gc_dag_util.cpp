/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_util.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_base.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_iterator.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

int ObSSGCDagUtil::check_if_stopped(const ObITask &task)
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler *);
  ObIDag *dag = task.get_dag();
  if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dag scheduler is null", KR(ret));
  } else if (dag_scheduler->has_set_stop()) {
    ret = OB_SERVICE_STOPPED;
    LOG_INFO("tenant dag scheduler has stopped", KR(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared storage gc dag is null", KR(ret));
  } else if (OB_FAIL(ObSSGCDagBase::check_if_stopped_or_failed(*dag))) {
    LOG_INFO("stop shared storage gc dag task", KR(ret),
        "dag_type", dag->get_type(), KP(dag));
  }
  return ret;
}

int ObSSGCDagUtil::get_all_ls_ids_from_meta_ctrl(
    const uint64_t tenant_id,
    ObMySQLProxy &sql_proxy,
    const int64_t sql_timeout_us,
    ObIArray<ObLSID> &ls_ids,
    const ObITask *task)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlLSIterator ls_iter;
  ls_ids.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ls_iter.init(tenant_id, sql_proxy, sql_timeout_us))) {
    LOG_WARN("fail to init shared storage gc ls iterator", KR(ret),
        K(tenant_id), K(sql_timeout_us));
  } else {
    ObLSID ls_id;
    while (OB_SUCC(ret)) {
      if (OB_NOT_NULL(task) && OB_FAIL(check_if_stopped(*task))) {
        LOG_INFO("stop scanning shared storage gc ls ids", KR(ret),
            K(tenant_id));
      } else {
        const int iter_ret = ls_iter.get_next(ls_id);
        if (OB_ITER_END == iter_ret) {
          break;
        } else if (OB_SUCCESS != iter_ret) {
          ret = iter_ret;
          LOG_WARN("fail to get next shared storage gc ls id", KR(ret),
              K(tenant_id));
        } else if (OB_FAIL(ls_ids.push_back(ls_id))) {
          LOG_WARN("fail to append shared storage gc ls id", KR(ret),
              K(tenant_id), K(ls_id));
        }
      }
    }
  }
  return ret;
}

int ObSSGCDagUtil::serialize_meta_key(
    const ObSSMetaCtrlKey &ctrl_key,
    ObIAllocator &allocator,
    ObString &meta_key)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  meta_key.reset();
  if (OB_UNLIKELY(!ctrl_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shared storage gc control key", KR(ret), K(ctrl_key));
  } else if (OB_FAIL(ctrl_key.get_meta_key_serialize_size(buf_len))) {
    LOG_WARN("fail to get shared storage gc meta key size", KR(ret),
        K(ctrl_key));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate shared storage gc meta key", KR(ret),
        K(buf_len));
  } else if (OB_FAIL(ctrl_key.serialize_meta_key(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize shared storage gc meta key", KR(ret),
        K(ctrl_key));
  } else {
    meta_key.assign(buf, pos);
  }
  return ret;
}

int ObSSGCDagUtil::serialize_prefix_meta_key(
    const ObSSMetaCtrlKey &ctrl_key,
    ObIAllocator &allocator,
    ObString &meta_key)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  meta_key.reset();
  if (OB_UNLIKELY(!ctrl_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shared storage gc control key", KR(ret), K(ctrl_key));
  } else if (OB_FAIL(ctrl_key.get_prefix_meta_key_serialize_size(buf_len))) {
    LOG_WARN("fail to get shared storage gc prefix meta key size", KR(ret),
        K(ctrl_key));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate shared storage gc prefix meta key", KR(ret),
        K(buf_len));
  } else if (OB_FAIL(
      ctrl_key.serialize_prefix_meta_key(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize shared storage gc prefix meta key", KR(ret),
        K(ctrl_key));
  } else {
    meta_key.assign(buf, pos);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
