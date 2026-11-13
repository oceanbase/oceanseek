/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON
#include "ob_errsim_module_interface.h"
#include "lib/worker.h"

#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include "lib/ob_define.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace common {

int __attribute__((weak)) build_tenant_errsim_moulde(
    const uint64_t tenant_id,
    const int64_t config_version,
    const common::ObArray<ObFixedLengthString<ObErrsimModuleTypeHelper::MAX_TYPE_NAME_LENGTH>> &module_array,
    const double percentage)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  UNUSED(config_version);
  UNUSED(module_array);
  UNUSED(percentage);
  return ret;
}

bool __attribute__((weak)) is_errsim_module(
    const uint64_t tenant_id,
    const ObErrsimModuleType::TYPE &type)
{
  bool b_ret = false;
  UNUSED(tenant_id);
  UNUSED(type);
  return b_ret;
}

int record_tenant_errsim_event(
    const uint64_t tenant_id,
    const ObErrsimModuleType::TYPE type,
    const int32_t error)
{
  int ret = OB_SUCCESS;
  // Preserve the original type while disabling module injection throughout event recording.
  lib::ErrsimModuleGuard guard(ObErrsimModuleType::ERRSIM_MODULE_NONE);
  if (!ObErrsimModuleTypeHelper::is_valid(type)
      || ObErrsimModuleType::ERRSIM_MODULE_NONE == type || OB_SUCCESS == error) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObTenantErrsimEvent event;
    event.build_event(type, error);
    ret = add_tenant_errsim_event(tenant_id, event);
  }
  return ret;
}

int __attribute__((weak)) add_tenant_errsim_event(
    const uint64_t tenant_id,
    const ObTenantErrsimEvent &event)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  UNUSED(event);
  return ret;
}

} // common
} // oceanbase

