/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/catalog/ob_unified_catalog_config.h"
#include "lib/allocator/page_arena.h"
#include "lib/json/ob_json.h"
#include "share/ob_define.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
using namespace common;
namespace share
{
int ObUnifiedCatalogConfig::load(const uint64_t tenant_id, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get tenant configuration", K(ret), K(tenant_id));
  } else if (ObString(tenant_config->unified_catalog_config.str()).empty()) {
    ret = OB_NOT_INIT;
    LOG_WARN("unified catalog is not configured", K(ret), K(tenant_id));
  } else if (OB_FAIL(parse(ObString(tenant_config->unified_catalog_config.str()), allocator))) {
    LOG_WARN("failed to parse unified catalog configuration", K(ret), K(tenant_id));
  }
  return ret;
}

int ObUnifiedCatalogConfig::parse(const ObString &value, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator temp_allocator("UnifiedConfig");
  json::Parser parser;
  json::Value *data = nullptr;
  ObString endpoint;
  ObString realm;
  bool seen_endpoint = false;
  bool seen_realm = false;
  if (value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unified catalog configuration is empty", K(ret));
  } else if (OB_FAIL(parser.init(&temp_allocator))) {
    LOG_WARN("failed to initialize configuration parser", K(ret));
  } else if (OB_FAIL(parser.parse(value.ptr(), value.length(), data))) {
    LOG_WARN("failed to parse unified catalog configuration", K(ret));
  } else if (OB_ISNULL(data) || json::JT_OBJECT != data->get_type()
             || 2 != data->get_object().get_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unified catalog configuration requires endpoint and realm", K(ret));
  } else {
    json::Pair *node = data->get_object().get_first();
    for (int64_t i = 0; OB_SUCC(ret) && i < data->get_object().get_size();
         ++i, node = node->get_next()) {
      if (OB_ISNULL(node) || OB_ISNULL(node->value_)
          || json::JT_STRING != node->value_->get_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid unified catalog configuration value", K(ret));
      } else if (0 == node->name_.case_compare("endpoint") && !seen_endpoint) {
        endpoint = node->value_->get_string();
        seen_endpoint = true;
      } else if (0 == node->name_.case_compare("realm") && !seen_realm) {
        realm = node->value_->get_string();
        seen_realm = true;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unknown or duplicate unified catalog configuration field", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!seen_endpoint || !seen_realm) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("missing unified catalog configuration field", K(ret));
    } else if (OB_FAIL(validate_endpoint(endpoint))) {
      LOG_WARN("invalid unified catalog endpoint", K(ret));
    } else if (realm.empty() || realm.length() > MAX_REALM_LENGTH) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid unified catalog realm", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < realm.length(); ++i) {
        if ('\r' == realm.ptr()[i] || '\n' == realm.ptr()[i]) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid unified catalog realm", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(allocator, endpoint, endpoint_))) {
      LOG_WARN("failed to copy unified catalog endpoint", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, realm, realm_))) {
      LOG_WARN("failed to copy unified catalog realm", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalogConfig::validate_endpoint(const ObString &endpoint)
{
  int ret = OB_SUCCESS;
  int64_t authority_begin = 0;
  if (endpoint.empty() || endpoint.length() > OB_MAX_URI_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
  } else if (endpoint.prefix_match(HTTP_PREFIX)) {
    authority_begin = strlen(HTTP_PREFIX);
  } else if (endpoint.prefix_match(HTTPS_PREFIX)) {
    authority_begin = strlen(HTTPS_PREFIX);
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCC(ret)) {
    if (authority_begin >= endpoint.length()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      for (int64_t i = authority_begin; OB_SUCC(ret) && i < endpoint.length(); ++i) {
        const char ch = endpoint.ptr()[i];
        if ('/' == ch || '?' == ch || '#' == ch || '@' == ch
            || '\r' == ch || '\n' == ch) {
          ret = OB_INVALID_ARGUMENT;
        }
      }
    }
  }
  return ret;
}
} // namespace share
} // namespace oceanbase
