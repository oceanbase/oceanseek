/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/prewarm/ob_ss_base_prewarmer.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;

void ObSSBasePrewarmer::reset()
{
  stat_.reset();
}

int ObSSBasePrewarmer::set_max_available_size_for_macro_cache(const PolicyStatus &policy_status)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  int64_t macro_cache_max_available_size = 0;
  ObSSMacroCacheType macro_cache_type = ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK;
  if (PolicyStatus::AUTO == policy_status) {
    macro_cache_type = ObSSMacroCacheType::MACRO_BLOCK;
  }

  if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMacroCacheMgr is NULL", KR(ret));
  } else if (OB_FAIL(macro_cache_mgr->get_max_free_size_of_cache_type(macro_cache_type, macro_cache_max_available_size))) {
    LOG_WARN("fail to get available macro space for prewarm", KR(ret), K(macro_cache_type));
  } else {
    stat_.set_macro_cache_max_available_size(macro_cache_max_available_size);
  }
  return ret;
} 

int ObSSBasePrewarmer::set_max_available_size(const PolicyStatus &policy_status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_max_available_size_for_macro_cache(policy_status))) {
    LOG_WARN("fail to set max available size for macro cache", KR(ret), K(policy_status));
  }
  return ret;
}

int ObSSBasePrewarmer::open_micro_iterator(
    const blocksstable::MacroBlockId &macro_id,
    const char *macro_block_buf,
    const int64_t macro_block_size,
    blocksstable::ObMicroBlockBareIterator &micro_iter)
{
  int ret = OB_SUCCESS;
  micro_iter.reset();
  int64_t micro_block_cnt = -1;

  if (OB_ISNULL(macro_block_buf) || OB_UNLIKELY(!macro_id.is_valid() || macro_block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(macro_id), KP(macro_block_buf), K(macro_block_size));
  } else if (OB_FAIL(micro_iter.open(
      macro_block_buf, macro_block_size,
      true/*need_check_data_integrity*/, false/*need_deserialize*/))) {
    LOG_WARN("fail to open ObMicroBlockBareIterator",
        KR(ret), KP(macro_block_buf), K(macro_block_size));
  } else if (OB_FAIL(micro_iter.get_micro_block_count(micro_block_cnt))) {
    LOG_WARN("fail to get_micro_block_count", KR(ret), K(micro_iter));
  } else if (OB_UNLIKELY(micro_block_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro block cnt invalid", KR(ret), K(micro_block_cnt), K(micro_iter));
  }

  return ret;
}


int ObSSBasePrewarmer::check_prewarm_cache_type(const blocksstable::MacroBlockId &macro_id,
                                                 const char *macro_block_buf,
                                                 const int64_t macro_size,
                                                 const bool is_hot_tablet,
                                                 PrewarmLocalCacheType &local_cache_type)
{
  int ret = OB_SUCCESS;
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  const int64_t macro_cache_min_admit_block_size = (OB_NOT_NULL(local_cache_service)
      ? local_cache_service->get_macro_cache_min_admit_block_size()
      : ObSSLocalCacheParams::DEF_MACRO_CACHE_MIN_ADMIT_BLOCK_SIZE);
  local_cache_type = ObSSPrewarmLocalCacheType::SS_PREWARM_MAX_TYPE;
  if (SSObjUtil::is_shared(macro_id) && SSObjUtil::is_major(macro_id) && SSObjUtil::is_macro_data(macro_id)) {
    local_cache_type = ObSSPrewarmLocalCacheType::SS_PREWARM_MACRO_CACHE;
  } else if (SSObjUtil::is_tmp_file(macro_id)) {
    local_cache_type = ObSSPrewarmLocalCacheType::SS_PREWARM_MACRO_CACHE;
  } else if (macro_size >= macro_cache_min_admit_block_size) {
    local_cache_type = ObSSPrewarmLocalCacheType::SS_PREWARM_MACRO_CACHE;
  } else if (is_hot_tablet) {
    local_cache_type = ObSSPrewarmLocalCacheType::SS_PREWARM_MACRO_CACHE;
  } else {
    local_cache_type = ObSSPrewarmLocalCacheType::SS_PREWARM_SKIPPED_TYPE;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
