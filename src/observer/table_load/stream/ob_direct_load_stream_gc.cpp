/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/stream/ob_direct_load_stream_gc.h"

#include "lib/container/ob_array.h"
#include "logservice/ob_log_service.h"
#include "observer/table_load/stream/ob_direct_load_stream_service.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

ObDirectLoadStreamGc::ObDirectLoadStreamGc(const uint64_t tenant_id)
  : tenant_id_(tenant_id)
{
}

int ObDirectLoadStreamGc::run(ObDirectLoadStreamService &stream_service,
                              ObIDirectLoadStreamMetaStore &meta_store,
                              const int64_t now)
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  if (OB_UNLIKELY(now <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(check_sys_ls_leader_(is_leader))) {
    LOG_WARN("failed to check SYS LS role for direct load stream gc",
             KR(ret), K_(tenant_id));
  } else if (is_leader) {
    ret = run_(stream_service, meta_store, now);
  }
  return ret;
}

int ObDirectLoadStreamGc::run_for_test(
    ObDirectLoadStreamService &stream_service,
    ObIDirectLoadStreamMetaStore &meta_store,
    const int64_t now)
{
  return run_(stream_service, meta_store, now);
}

int ObDirectLoadStreamGc::run_(ObDirectLoadStreamService &stream_service,
                               ObIDirectLoadStreamMetaStore &meta_store,
                               const int64_t now)
{
  int ret = OB_SUCCESS;
  int first_ret = OB_SUCCESS;
  ObArray<ObDirectLoadStreamMeta> metas;
  if (OB_UNLIKELY(now <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(meta_store.scan_gc_streams(now, metas))) {
    LOG_WARN("failed to scan direct load stream gc candidates", KR(ret), K(now));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < metas.count(); ++i) {
    const int process_ret = process_stream_(
        stream_service, meta_store, metas.at(i), now);
    if (OB_SUCCESS != process_ret) {
      if (OB_SUCCESS == first_ret) {
        first_ret = process_ret;
      }
      LOG_WARN("failed to process direct load stream gc candidate",
               KR(process_ret), K_(tenant_id), K(i), K(metas.at(i)));
    }
  }
  if (OB_SUCC(ret)) {
    ret = first_ret;
  }
  return ret;
}

int ObDirectLoadStreamGc::process_stream_(
    ObDirectLoadStreamService &stream_service,
    ObIDirectLoadStreamMetaStore &meta_store,
    const ObDirectLoadStreamMeta &meta,
    const int64_t now)
{
  int ret = OB_SUCCESS;
  bool finished = false;
  bool cleanup_ready = DIRECT_LOAD_STREAM_STATUS_FINISHED == meta.status_;
  ObDirectLoadStreamMeta latest_meta;
  if (OB_UNLIKELY(now <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(DIRECT_LOAD_STREAM_STATUS_OPEN != meta.status_
                         && DIRECT_LOAD_STREAM_STATUS_SEALED != meta.status_
                         && DIRECT_LOAD_STREAM_STATUS_FINISHED != meta.status_)) {
    ret = OB_STATE_NOT_MATCH;
  } else if (!cleanup_ready
             && OB_FAIL(meta_store.finish_expired_unclaimed_stream(
                 meta.stream_id_, now, finished))) {
    LOG_WARN("failed to finish expired unclaimed stream gc candidate",
             KR(ret), K_(tenant_id), K(meta));
  } else {
    cleanup_ready = cleanup_ready || finished;
  }
  if (OB_SUCC(ret) && cleanup_ready) {
    const int result_code = DIRECT_LOAD_STREAM_STATUS_FINISHED == meta.status_
        ? meta.result_code_ : OB_TIMEOUT;
    if (OB_FAIL(stream_service.finish_and_cleanup_stream(
                    meta.stream_id_, result_code))) {
      LOG_WARN("failed to finish and cleanup direct load stream gc candidate",
               KR(ret), K_(tenant_id), K(meta.stream_id_), K(result_code));
    }
  }
  if (OB_SUCC(ret) && cleanup_ready
      && OB_FAIL(meta_store.get_stream(meta.stream_id_, latest_meta))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to reload cleaned direct load stream metadata",
               KR(ret), K_(tenant_id), K(meta.stream_id_));
    }
  } else if (OB_SUCC(ret) && cleanup_ready
             && OB_UNLIKELY(DIRECT_LOAD_STREAM_STATUS_FINISHED != latest_meta.status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cleaned direct load stream metadata is not terminal",
             KR(ret), K_(tenant_id), K(latest_meta));
  } else if (OB_SUCC(ret) && cleanup_ready
             && OB_FAIL(meta_store.delete_stream(
                 latest_meta.stream_id_, latest_meta.modified_ts_))) {
    LOG_WARN("failed to delete cleaned direct load stream metadata",
             KR(ret), K_(tenant_id), K(latest_meta));
  }
  return ret;
}

int ObDirectLoadStreamGc::check_sys_ls_leader_(bool &is_leader)
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService *);
  ObRole role = ObRole::INVALID_ROLE;
  int64_t proposal_id = palf::INVALID_PROPOSAL_ID;
  is_leader = false;
  if (OB_ISNULL(log_service)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int role_ret =
        log_service->get_palf_role(share::SYS_LS, role, proposal_id);
    if (OB_LS_NOT_EXIST == role_ret) {
      // A tenant service can run on a server that does not host the tenant's
      // SYS LS. The local task silently skips global stream metadata GC.
    } else if (OB_SUCCESS != role_ret) {
      ret = role_ret;
      LOG_WARN("failed to get SYS LS role for direct load stream gc",
               KR(ret), K_(tenant_id));
    } else {
      is_leader = is_leader_by_election(role);
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
