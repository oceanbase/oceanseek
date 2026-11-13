/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_table_creator_privilege.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_cluster_version.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
constexpr ObPrivSet ObTableCreatorPrivilege::PRIVILEGES;

int ObTableCreatorPrivilege::check_enabled(const uint64_t tenant_id, bool &enabled)
{
  int ret = OB_SUCCESS;
  enabled = false;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_6_3_1) {
    omt::ObTenantConfigGuard config(OTC_MGR.get_tenant_config_with_lock(tenant_id));
    if (!config.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant configuration is unavailable", KR(ret), K(tenant_id));
    } else {
      enabled = config->_enable_auto_grant_to_table_creator;
    }
  }
  return ret;
}

bool ObTableCreatorPrivilege::is_supported_table(const ObTableSchema &schema)
{
  return schema.is_user_table() && !schema.is_tmp_table()
      && !schema.is_ctas_tmp_table() && !schema.is_user_hidden_table();
}

} // namespace schema
} // namespace share
} // namespace oceanbase
