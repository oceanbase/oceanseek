/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SHARE_ERRSIM_MODULE_OB_TENANT_ERRSIM_MODULE_MGR_H_
#define SRC_SHARE_ERRSIM_MODULE_OB_TENANT_ERRSIM_MODULE_MGR_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "common/errsim_module/ob_errsim_module_type.h"
#include "lib/container/ob_array.h"
#include "lib/lock/ob_spin_rwlock.h"

namespace oceanbase
{
namespace share
{

class ObTenantErrsimModuleMgr
{
public:
  typedef ObFixedLengthString<ObErrsimModuleTypeHelper::MAX_TYPE_NAME_LENGTH> ErrsimModuleString;
  typedef ObArray<ObFixedLengthString<ObErrsimModuleTypeHelper::MAX_TYPE_NAME_LENGTH>> ModuleArray;
public:
  ObTenantErrsimModuleMgr();
  virtual ~ObTenantErrsimModuleMgr();
  static int mtl_init(ObTenantErrsimModuleMgr *&errsim_module_mgr);
  int init(const uint64_t tenant_id);
  int build_tenant_moulde(
      const uint64_t tenant_id,
      const int64_t config_version,
      const ModuleArray &module_array,
      const double percentage);
  bool is_errsim_module(
      const ObErrsimModuleType::TYPE &type);
  void destroy();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::SpinRWLock lock_;
  int64_t config_version_;
  bool enabled_modules_[ObErrsimModuleType::ERRSIM_MODULE_MAX];
  double percentage_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantErrsimModuleMgr);
};

} // namespace share
} // namespace oceanbase
#endif
