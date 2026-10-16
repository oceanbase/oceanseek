// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#include "storage/compaction/ob_major_pre_warmer.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
namespace oceanbase
{
using namespace blocksstable;
using namespace storage;
namespace compaction
{

bool ObMajorPreWarmerParam::is_valid() const
{
  return ObPreWarmerParam::is_valid() && is_inited_;
}

int ObMajorPreWarmerParam::init(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const bool use_fixed_percentage)
{
  int ret = OB_SUCCESS;
  UNUSED(ls_id);
  UNUSED(tablet_id);
  UNUSED(use_fixed_percentage);
  PolicyStatus policy_status = PolicyStatus::MAX_STATUS;
  ObStorageCachePolicyService *storage_cache_policy_service = nullptr;
  if (is_valid_tenant_id(MTL_ID())) {
    if (OB_ISNULL(storage_cache_policy_service = MTL(ObStorageCachePolicyService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "storage cache policy service is null", KR(ret));
    } else if (OB_FAIL(storage_cache_policy_service->get_tablet_policy_status(tablet_id.id(), policy_status))) {
      STORAGE_LOG(WARN, "fail to check if hot tablet", KR(ret), K(tablet_id));
    } else if (policy_status == PolicyStatus::HOT) {
      pre_warm_level_ = ObSSMajorPrewarmLevel::PREWARM_META_AND_DATA_LEVEL;
    } else if (policy_status == PolicyStatus::COLD) {
      pre_warm_level_ = ObSSMajorPrewarmLevel::PREWARM_NONE_LEVEL;
    } else if (policy_status == PolicyStatus::AUTO || policy_status == PolicyStatus::MACRO_HOT) {
      get_prewarm_level_from_tenant_config();
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid policy status", KR(ret), K(policy_status), K(tablet_id));
    }
  } else {
    get_prewarm_level_from_tenant_config();
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

void ObMajorPreWarmerParam::get_prewarm_level_from_tenant_config()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (OB_LIKELY(tenant_config.is_valid())) {
    pre_warm_level_ = static_cast<ObSSMajorPrewarmLevel>(static_cast<uint8_t>(
                      tenant_config->_major_compaction_prewarm_level));
  } else {
    pre_warm_level_ = ObSSMajorPrewarmLevel::PREWARM_META_AND_DATA_LEVEL;
    STORAGE_LOG(INFO, "invalid tenant config, use default pre warm level", K_(pre_warm_level));
  }
}

} // namespace compaction
} // namespace oceanbase
