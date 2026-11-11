/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_lob_retry.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "share/location_cache/ob_location_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/lob/common/ob_lob_location.h"

namespace oceanbase
{
namespace storage
{

int check_tenant_status(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard guard;
  share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  const share::schema::ObTenantSchema *tenant_schema = nullptr;
  bool is_dropped = false;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
    LOG_WARN("check if tenant has been dropped failed", KR(ret), K(tenant_id));
  } else if (is_dropped) {
    ret = OB_TENANT_HAS_BEEN_DROPPED;
    LOG_WARN("tenant has been dropped", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("get sys tenant schema guard failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("get tenant info failed", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is null", KR(ret), K(tenant_id), KP(tenant_schema));
  } else if (tenant_schema->is_dropping()) {
    ret = OB_LS_TENANT_DROPPING;
    LOG_WARN("tenant is dropping", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObLobRetryUtil::check_need_retry(
    ObLobReadContext &read_ctx, const int error_code, const int retry_cnt, bool &need_retry)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = THIS_WORKER.get_session();
  lib::Worker::Status worker_status = lib::Worker::WS_NOWAIT;
  need_retry = false;
  if (read_ctx.from_rpc_ && !read_ctx.enable_remote_retry_) {
    LOG_WARN("can not retry because from rpc", KR(error_code), K(need_retry), K(read_ctx));
  } else if (!is_remote_ret_can_retry(error_code)) {
    LOG_WARN("can not retry error code", KR(error_code), K(retry_cnt), K(need_retry), K(read_ctx));
  } else if (read_ctx.src_loc_.is_lob_disk_locator()) {
    LOG_WARN("can not retry because src loc is lob disk locator", KR(error_code), K(retry_cnt),
        K(need_retry), K(read_ctx));
  } else if (ObTimeUtility::current_time() > read_ctx.timeout_) {
    int64_t cur_time = ObTimeUtility::current_time();
    LOG_WARN("[LOB RETRY] query timeout", KR(error_code), K(retry_cnt),
        K(cur_time), K(read_ctx.timeout_), K(need_retry));
  } else if (OB_NOT_NULL(session) && session->is_terminate(ret)) {
    LOG_INFO("[LOB RETRY] retry is interrupted by session status", KR(error_code), K(need_retry), K(retry_cnt));
  } else if (IS_INTERRUPTED()) {
    LOG_INFO("[LOB RETRY] retry is interrupted", KR(error_code), K(retry_cnt), K(need_retry));
  } else if (lib::Worker::WS_INVALID == (worker_status = THIS_WORKER.check_wait())) {
    ret = THIS_WORKER.is_th_worker() ? OB_TENANT_NOT_IN_SERVER : OB_CANCELED;
    LOG_INFO("[LOB RETRY] worker cannot continue", KR(error_code), K(retry_cnt));
  } else if (lib::Worker::WS_OUT_OF_THROTTLE == worker_status) {
    ret = OB_KILLED_BY_THROTTLING;
    LOG_INFO("[LOB RETRY] retry is interrupted by throttling", KR(error_code), K(retry_cnt));
  } else if (OB_FAIL(check_tenant_status(read_ctx.tenant_id_))) {
    LOG_WARN("check tenant status before lob retry failed", KR(ret), K(error_code), K(retry_cnt), K(read_ctx));
  } else {
    switch (error_code) {
      case OB_TENANT_NOT_IN_SERVER:
      case OB_LS_NOT_EXIST:
      case OB_LS_OFFLINE:
      case OB_NOT_MASTER:
      case OB_TABLET_NOT_EXIST:
      case OB_NO_READABLE_REPLICA:
       {
        if (OB_ISNULL(GCTX.location_service_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("location service is null", KR(ret), K(read_ctx));
        } else if (OB_FAIL(GCTX.location_service_->renew_tablet_location(
            read_ctx.tenant_id_, read_ctx.tablet_id_, error_code, false/*is_nonblock*/))) {
          LOG_WARN("fail to renew tablet location", KR(ret), K(error_code), K(retry_cnt), K(read_ctx));
        }
      } // fall through
      case OB_REPLICA_NOT_READABLE:
      case OB_RPC_CONNECT_ERROR:
      case OB_RPC_SEND_ERROR:
      case OB_RPC_POST_ERROR:
      {
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObLobLocationUtil::lob_refresh_location(read_ctx, error_code, retry_cnt))) {
          LOG_WARN("fail to refresh lob location", KR(ret), K(error_code), K(retry_cnt), K(read_ctx));
        } else {
          need_retry = true;
        }
        LOG_INFO("[LOB RETRY] checked retry", KR(ret), K(error_code), K(retry_cnt), K(need_retry), K(read_ctx));
        break;
      }
      default: {
        LOG_WARN("unknown retry error code", KR(ret), KR(error_code), K(retry_cnt), K(need_retry));
      }
    }
  }
  return ret;
}

bool ObLobRetryUtil::is_remote_ret_can_retry(int ret)
{
  return (ret == OB_REPLICA_NOT_READABLE) || 
         (ret == OB_RPC_CONNECT_ERROR) ||
         (ret == OB_RPC_SEND_ERROR) || 
         (ret == OB_RPC_POST_ERROR) || 
         (ret == OB_NOT_MASTER) || 
         (ret == OB_NO_READABLE_REPLICA) || 
         (ret == OB_TABLET_NOT_EXIST) || 
         (ret == OB_LS_NOT_EXIST) ||
         (ret == OB_TENANT_NOT_IN_SERVER) ||
         (ret == OB_LS_OFFLINE);
}

} // storage
} // oceanbase
