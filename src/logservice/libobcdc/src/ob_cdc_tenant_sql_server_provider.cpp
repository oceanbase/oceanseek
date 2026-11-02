/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_tenant_sql_server_provider.h"

#include "ob_log_config.h"                              // ObLogConfig
#include "ob_log_systable_helper.h"                     // ObLogSysTableHelper
#include "ob_log_svr_blacklist.h"                       // ObLogSvrBlacklist

namespace oceanbase
{
namespace libobcdc
{

ObCDCTenantSQLServerProvider::ObCDCTenantSQLServerProvider()
  : is_inited_(false),
    systable_helper_(NULL),
    tenant_list_(ObModIds::OB_LOG_SERVER_PROVIDER ,OB_MALLOC_NORMAL_BLOCK_SIZE),
    server_list_(ObModIds::OB_LOG_SERVER_PROVIDER ,OB_MALLOC_NORMAL_BLOCK_SIZE),
    tenant_server_map_(),
    server_blacklist_(),
    refresh_server_lock_(common::ObLatchIds::OB_CDC_TENANT_REFRESH_SERVER_LOCK)
{}

int ObCDCTenantSQLServerProvider::init(IObLogSysTableHelper &systable_helper)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant_sql_server_provider already inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(tenant_server_map_.init(ObModIds::OB_SQL_CONNECTION_POOL))) {
    LOG_ERROR("tenant_server_map_ init failed", KR(ret));
  } else {
    systable_helper_ = &systable_helper;
    is_inited_ = true;
  }

  return ret;
}

void ObCDCTenantSQLServerProvider::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    systable_helper_ = NULL;
    tenant_list_.destroy();
    server_list_.destroy();
    tenant_server_map_.reset();
    server_blacklist_.destroy();
  }
}

void ObCDCTenantSQLServerProvider::configure(const ObLogConfig &config)
{
  const char *sql_server_blacklist = config.sql_server_blacklist.str();
  LOG_INFO("[CONFIG]", KCSTRING(sql_server_blacklist));
  server_blacklist_.refresh(sql_server_blacklist);
}

int ObCDCTenantSQLServerProvider::del_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("cdc_tenant_server_provider not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_server_map_.del(tenant_id))) {
    LOG_WARN("del tenant_server_info from tenant_server_map_ failed", KR(ret), K(tenant_id));
  } else {
    // success
  }

  return ret;
}

int ObCDCTenantSQLServerProvider::prepare_refresh()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant_sql_server_provider not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(refresh_server_lock_.wrlock())) { // lock with default timeout(INT64_MAX)
    LOG_ERROR("lock refresh_server_lock_ failed", KR(ret));
  } else if (OB_FAIL(query_all_server_())) {
    LOG_WARN("query_all_server_ failed", KR(ret));
  } else if (OB_FAIL(query_all_tenant_())) {
    LOG_WARN("query_all_tenant_ failed", KR(ret));
  } else if (OB_FAIL(query_tenant_server_())) {
    LOG_WARN("query_tenant_server_ failed", KR(ret));
  }
  LOG_INFO("prepare_refresh tenant sql_server_provider done", KR(ret));

  return ret;
}

int ObCDCTenantSQLServerProvider::end_refresh()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant_sql_server_provider not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(refresh_server_lock_.unlock())) {
    LOG_ERROR("release refresh_server_lock_ failed", KR(ret), K(this));
  }
  LOG_INFO("refresh tenant sql_server_provider end", KR(ret));

  return ret;
}

int ObCDCTenantSQLServerProvider::get_server(const int64_t svr_idx, common::ObAddr &server)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_sql_server_provider is not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(svr_idx >= get_server_count())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("request server is not exist", KR(ret),
        K(svr_idx), "server_count", get_server_count());
  } else if (OB_FAIL(server_list_.at(svr_idx, server))) {
    LOG_ERROR("get server at specified position failed", KR(ret), K(svr_idx), K_(server_list));
  } else {
    // success
  }

  return ret;
}

int64_t ObCDCTenantSQLServerProvider::get_server_count() const
{
  int64_t server_count = 0;

  if (IS_INIT) {
    server_count = server_list_.count();
  }

  return server_count;
}

int ObCDCTenantSQLServerProvider::get_tenant_ids(common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_sql_server_provider is not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(tenant_ids.assign(tenant_list_))) {
    LOG_WARN("assign tenant_list_ to tenant_ids failed", KR(ret), K(tenant_list_), K(tenant_ids));
  }

  return ret;
}

int ObCDCTenantSQLServerProvider::get_tenant_servers(
    const uint64_t tenant_id,
    common::ObIArray<common::ObAddr> &tenant_servers)
{
  int ret = OB_SUCCESS;
  tenant_servers.reset();
  TenantServerList *tenant_server_list = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_sql_server_provider is not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(tenant_server_map_.get(tenant_id, tenant_server_list))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("tenant not exist in tenant_server_map_", KR(ret), K(tenant_id));
    }
  } else if (OB_FAIL(tenant_servers.assign(tenant_server_list->get_server_list()))) {
    LOG_ERROR("assign tenant_server_list failed", KR(ret), K(tenant_id), K(tenant_server_list));
  }

  if (OB_NOT_NULL(tenant_server_list)) {
    tenant_server_map_.revert(tenant_server_list);
  }

  return ret;
}

int ObCDCTenantSQLServerProvider::refresh_server_list(void)
{
  return OB_SUCCESS;
}

// NOTICE: query into a temporary list and only replace tenant_list_ on success, so that a
// transient query failure (e.g. sys tenant busy) won't drop the tenant list cached by the last
// successful refresh.
int ObCDCTenantSQLServerProvider::query_all_tenant_()
{
  int ret = OB_SUCCESS;
  TenantList tenant_list(ObModIds::OB_LOG_SERVER_PROVIDER, OB_MALLOC_NORMAL_BLOCK_SIZE);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_sql_server_provider is not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(systable_helper_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("systable_helper_ should not be null", KR(ret));
  } else if (OB_FAIL(systable_helper_->query_tenant_id_list(tenant_list))) {
    LOG_WARN("get tenant_id_list by systable_helper_ failed, keep the cached tenant_list",
        KR(ret), K_(tenant_list));
  } else if (OB_UNLIKELY(0 == tenant_list.count())) {
    ret = OB_NEED_RETRY;
    LOG_WARN("tenant_id_list query from cluster is empty, keep the cached tenant_list",
        KR(ret), K_(tenant_list));
  } else if (OB_FAIL(tenant_list_.assign(tenant_list))) {
    LOG_ERROR("assign tenant_list failed", KR(ret), K(tenant_list));
  } else {
    // success
  }

  return ret;
}

// NOTICE: same as query_all_tenant_, keep the cached server_list_ if query failed or got empty.
int ObCDCTenantSQLServerProvider::query_all_server_()
{
  int ret = OB_SUCCESS;
  ServerList server_list(ObModIds::OB_LOG_SERVER_PROVIDER, OB_MALLOC_NORMAL_BLOCK_SIZE);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_sql_server_provider is not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(systable_helper_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("systable_helper_ should not be null", KR(ret));
  } else if (OB_FAIL(systable_helper_->query_sql_server_list(server_blacklist_, server_list))) {
    LOG_WARN("query_server_list failed, keep the cached server_list", KR(ret), K_(server_list));
  } else if (OB_UNLIKELY(0 == server_list.count())) {
    ret = OB_NEED_RETRY;
    LOG_WARN("server_list query from cluster is empty, keep the cached server_list",
        KR(ret), K_(server_list));
  } else if (OB_FAIL(server_list_.assign(server_list))) {
    LOG_ERROR("assign server_list failed", KR(ret), K(server_list));
  } else {
    // success
  }

  return ret;
}

// Refresh tenant_server_map_ for each tenant in tenant_list_:
// 1. query succ and server list is not empty: replace the cached server list of the tenant.
// 2. query succ but server list is empty:
//    (1) tenant is dropped: remove the tenant from tenant_server_map_, then the connection pool
//        will skip the tenant(get_tenant_servers returns OB_ENTRY_NOT_EXIST).
//    (2) otherwise(e.g. unit migrating, server lease expired or not start service yet): keep
//        the cached server list.
// 3. query failed(e.g. sys tenant busy): keep the cached server list.
// The cached server list of a tenant is never cleared before a new valid server list is fetched,
// otherwise the connection pool will find an empty server list and fail to renew the
// tenant connection pool with OB_ERR_UNEXPECTED.
// Failure of one tenant won't affect others, the first error is returned.
int ObCDCTenantSQLServerProvider::query_tenant_server_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_sql_server_provider is not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(systable_helper_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("systable_helper_ should not be null", KR(ret));
  } else if (OB_UNLIKELY(0 == server_list_.count() || 0 == tenant_list_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect valid server_list and tenant_list", KR(ret), K_(server_list), K_(tenant_list));
  } else {
    for (int64_t tenant_idx = 0; tenant_idx < tenant_list_.count(); tenant_idx++) {
      int tmp_ret = OB_SUCCESS;
      const uint64_t tenant_id = tenant_list_.at(tenant_idx);

      if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expect valid tenant_id", KR(tmp_ret), K_(tenant_list), K(tenant_idx), K(tenant_id));
      } else if (OB_TMP_FAIL(refresh_tenant_server_list_(tenant_id))) {
        LOG_WARN("refresh tenant_server_list failed", KR(tmp_ret), K(tenant_id));
      }

      if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
        ret = tmp_ret;
      }
    }
  }

  return ret;
}

int ObCDCTenantSQLServerProvider::refresh_tenant_server_list_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ServerList tenant_servers(ObModIds::OB_LOG_SERVER_PROVIDER, OB_MALLOC_NORMAL_BLOCK_SIZE);

  if (OB_FAIL(systable_helper_->query_tenant_sql_server_list(tenant_id, tenant_servers))) {
    LOG_WARN("query_tenant_sql_server_list by systable_helper failed, keep the cached tenant_server_list",
        KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(0 == tenant_servers.count())) {
    share::schema::TenantStatus tenant_status = share::schema::TenantStatus::TENANT_STATUS_INVALID;

    if (OB_FAIL(systable_helper_->query_tenant_status(tenant_id, tenant_status))) {
      LOG_WARN("query_tenant_status failed, keep the cached tenant_server_list", KR(ret), K(tenant_id));
    } else if (share::schema::TenantStatus::TENANT_DELETED == tenant_status) {
      if (OB_FAIL(tenant_server_map_.del(tenant_id))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("del tenant_server_list from tenant_server_map_ failed", KR(ret), K(tenant_id));
        }
      }
      LOG_INFO("tenant already dropped, remove tenant_server_list", KR(ret), K(tenant_id), K(tenant_status));
    } else {
      LOG_WARN("tenant_server_list is empty but tenant is not dropped, keep the cached tenant_server_list",
          K(tenant_id), K(tenant_status));
    }
  } else {
    TenantServerList *tenant_server_list = NULL;

    if (OB_FAIL(tenant_server_map_.get(tenant_id, tenant_server_list))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(tenant_server_map_.create(tenant_id, tenant_server_list))) {
          LOG_ERROR("create tenant_server_list failed", KR(ret), K(tenant_id));
        }
      } else {
        LOG_ERROR("get tenant_server_list from tenant_server_map_ failed", KR(ret), K(tenant_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(tenant_server_list)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant_server_list get/created from tenant_server_map_ should not be null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(tenant_server_list->get_server_list().assign(tenant_servers))) {
      LOG_ERROR("assign tenant_server_list failed", KR(ret), K(tenant_id), K(tenant_servers));
    } else {
      LOG_INFO("find tenant servers", K(tenant_id), KPC(tenant_server_list));
    }

    if (OB_NOT_NULL(tenant_server_list)) {
      // revert tenant_server_list anyway cause already get from tenant_server_map_ succ.
      tenant_server_map_.revert(tenant_server_list);
    }
  }

  return ret;
}

}// end namespace libobcdc
} // end namespace oceanbase
