/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/tx/ob_ts_mgr.h"
#include "share/ob_tenant_info_proxy.h"
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

namespace oceanbase
{
namespace storage
{

int ObAtomicProtocolUtil::get_gts(const uint64_t tenant_id, share::SCN &gts)
{
  return get_gts(tenant_id, DEFAULT_GET_GTS_TIMEOUT_US, gts);
}

int ObAtomicProtocolUtil::get_gts(const uint64_t tenant_id, const int64_t timeout_us, share::SCN &gts)
{
  return get_gts_(tenant_id, timeout_us, false/*use_sync_get*/, gts);
}

int ObAtomicProtocolUtil::get_gts_sync(const uint64_t tenant_id, share::SCN &gts)
{
  return get_gts_sync(tenant_id, DEFAULT_GET_GTS_TIMEOUT_US, gts);
}

int ObAtomicProtocolUtil::get_gts_sync(const uint64_t tenant_id, const int64_t timeout_us, share::SCN &gts)
{
  return get_gts_(tenant_id, timeout_us, true/*use_sync_get*/, gts);
}

int ObAtomicProtocolUtil::get_gts_(
    const uint64_t tenant_id,
    const int64_t timeout_us,
    const bool use_sync_get,
    share::SCN &gts)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("real tenant id is invalid", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid get gts timeout", KR(ret), K(tenant_id), K(timeout_us));
  } else if (OB_FAIL(get_real_tenant_id(tenant_id, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K(tenant_id));
  } else {
    const transaction::MonotonicTs stc = transaction::MonotonicTs::current_time();
    transaction::MonotonicTs unused_ts(0);
    const int64_t start_time_us = ObTimeUtility::current_time_us();
    int64_t retry_count = 0;
    ret = OB_EAGAIN;
    do {
      const int64_t elapsed_us = ObTimeUtility::current_time_us() - start_time_us;
      const int64_t left_timeout_us = timeout_us - elapsed_us;
      if (left_timeout_us <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("get gts timeout", KR(ret), K(tenant_id), K(real_tenant_id), K(stc),
          K(timeout_us), K(use_sync_get), K(retry_count));
      } else if (use_sync_get) {
        if (OB_FAIL(OB_TS_MGR.get_gts_sync(real_tenant_id, stc, left_timeout_us, gts, unused_ts))) {
          LOG_WARN("failed to get gts sync", KR(ret), K(tenant_id), K(real_tenant_id), K(stc),
            K(timeout_us), K(use_sync_get), K(retry_count));
        }
      } else {
        if (OB_FAIL(OB_TS_MGR.get_gts(real_tenant_id, stc, NULL, gts, unused_ts))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("failed to get gts", KR(ret), K(tenant_id), K(real_tenant_id), K(stc),
              K(timeout_us), K(use_sync_get), K(retry_count));
          }
        }
      }

      if (OB_FAIL(ret)) {
        const int64_t left_after_fail_us = timeout_us - (ObTimeUtility::current_time_us() - start_time_us);
        if (left_after_fail_us > 0 && (use_sync_get || OB_EAGAIN == ret)) {
          ++retry_count;
          ob_usleep(left_after_fail_us > 10L * 1000L ? 10L * 1000L : left_after_fail_us);
        }
      }
    } while (OB_FAIL(ret)
        && (use_sync_get || OB_EAGAIN == ret)
        && (ObTimeUtility::current_time_us() - start_time_us) < timeout_us);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get gts", KR(ret), K(tenant_id), K(real_tenant_id), K(stc),
          K(timeout_us), K(use_sync_get), K(retry_count));
    }
  }
  return ret;
}

int ObAtomicProtocolUtil::get_real_tenant_id(
    const uint64_t tenant_id,
    uint64_t &real_tenant_id)
{
  int ret = OB_SUCCESS;
  real_tenant_id = OB_INVALID_ID;
  if (is_user_tenant(tenant_id)) {
    real_tenant_id = gen_meta_tenant_id(tenant_id);
  } else if (is_meta_tenant(tenant_id)) {
    real_tenant_id = OB_SYS_TENANT_ID;
  } else if (is_sys_tenant(tenant_id)) {
    real_tenant_id = tenant_id;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant type is unknow, unexpected", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObAtomicProtocolUtil::start_trans(
    const uint64_t tenant_id, 
    const int64_t stmt_timeout, 
    const bool with_snapshot,
    const int32_t group_id,
    common::ObTimeoutCtx &timeout_ctx, 
    common::ObMySQLTransaction &trans) 
{  
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (stmt_timeout <= 0 || group_id < 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start trans get invalid argument", K(ret), K(stmt_timeout), K(group_id), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(get_real_tenant_id(tenant_id, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy, real_tenant_id, with_snapshot, group_id))) {
    LOG_WARN("failed to start trans", K(ret));
  }
  return ret;
}

int ObAtomicProtocolUtil::commit_trans(
    const int32_t &result,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  tmp_ret = trans.end(OB_SUCCESS == result);
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

int ObAtomicProtocolUtil::get_trans_timeout_us(const uint64_t tenant_id, int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  timeout_us = DEFAULT_ATOMIC_PROTOCOL_TRANS_TIMEOUT_US;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_LIKELY(tenant_config.is_valid())) {
      timeout_us = tenant_config->_atomic_protocol_trans_timeout;
    }
  }
  return ret;
}


} // storage
} // oceanbase
