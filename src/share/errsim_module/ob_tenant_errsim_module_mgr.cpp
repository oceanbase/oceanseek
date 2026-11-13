/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON
#include "ob_tenant_errsim_module_mgr.h"
#include "lib/random/ob_random.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace lib;

namespace share
{

ObTenantErrsimModuleMgr::ObTenantErrsimModuleMgr()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      lock_(common::ObLatchIds::ERRSIM_LOCK),
      config_version_(0),
      enabled_modules_{},
      percentage_(0)
{
}

ObTenantErrsimModuleMgr::~ObTenantErrsimModuleMgr()
{
}

int ObTenantErrsimModuleMgr::mtl_init(ObTenantErrsimModuleMgr *&errsim_module_mgr)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(errsim_module_mgr->init(tenant_id))) {
    LOG_WARN("failed to init errsim module mgr", K(ret), KP(errsim_module_mgr));
  }
  return ret;
}

void ObTenantErrsimModuleMgr::destroy()
{
  common::SpinWLockGuard guard(lock_);
  is_inited_ = false;
  tenant_id_ = OB_INVALID_ID;
  config_version_ = 0;
  MEMSET(enabled_modules_, 0, sizeof(enabled_modules_));
  percentage_ = 0;
}

int ObTenantErrsimModuleMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant errsim module mgr init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tenant errsim module mgr get invalid argument", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

bool ObTenantErrsimModuleMgr::is_errsim_module(
    const ObErrsimModuleType::TYPE &type)
{
  bool b_ret = false;
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant errsim module mgr do not init", K(ret));
  } else if (!ObErrsimModuleTypeHelper::is_valid(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is errsim module get invalid argument", K(ret), K(type));
  } else {
    common::SpinRLockGuard guard(lock_);
    // Sample in [0, 100), retaining fractional percentages and exact 0%/100% boundaries.
    const int64_t random_range = static_cast<int64_t>(INT32_MAX) + 1;
    const double percentage = 100.0 * ObRandom::rand(0, random_range - 1) / random_range;
    if (percentage >= percentage_) {
      b_ret = false;
    } else {
      b_ret = enabled_modules_[ObErrsimModuleType::ERRSIM_MODULE_ALL] || enabled_modules_[type];
    }
  }
  return b_ret;
}

int ObTenantErrsimModuleMgr::build_tenant_moulde(
    const uint64_t tenant_id,
    const int64_t config_version,
    const ModuleArray &module_array,
    const double percentage)
{
  int ret = OB_SUCCESS;
  bool enabled_modules[ObErrsimModuleType::ERRSIM_MODULE_MAX] = {};
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant errsim module mgr do not init", K(ret));
  } else if (tenant_id != tenant_id_ || config_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build tenant module get invalid argument", K(ret), K(tenant_id), K(config_version));
  } else if (!(percentage >= 0 && percentage <= 100)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid errsim module percentage", K(ret), K(percentage));
  } else {
    // Build a complete snapshot without modifying the active configuration.
    for (int64_t i = 0; OB_SUCC(ret) && i < module_array.size(); ++i) {
      const ErrsimModuleString &name = module_array.at(i);
      const ObErrsimModuleType::TYPE type = ObErrsimModuleTypeHelper::get_type(name.ptr());
      if (!ObErrsimModuleTypeHelper::is_valid(type)) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("invalid errsim module type", K(ret), K(name));
      } else {
        enabled_modules[type] = true;
      }
    }
    if (OB_SUCC(ret)) {
      common::SpinWLockGuard guard(lock_);
      if (config_version > config_version_) {
        // Publishing cannot allocate or fail. Readers see one complete version.
        MEMCPY(enabled_modules_, enabled_modules, sizeof(enabled_modules_));
        percentage_ = percentage;
        config_version_ = config_version;
        LOG_INFO("updated tenant errsim modules", K(tenant_id), K(config_version), K(module_array), K(percentage));
      }
    }
  }
  return ret;
}


} //share
} //oceanbase
