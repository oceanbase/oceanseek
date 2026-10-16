/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_evict_policy.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;

int ObSSMacroCacheDefaultEvictPolicy::get_evict_candidates(
    ObIArray<ObSSMacroCacheEvictEntry> &evict_entries)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObMemAttr attr(MTL_ID(), "EvictPolicy");
  ObSEArray<ObSSMacroCacheType, SS_MACRO_CACHE_MAX_TYPE_VAL> macro_cache_types;
  macro_cache_types.set_attr(attr);
  ObSEArray<int64_t/*score*/, SS_MACRO_CACHE_MAX_TYPE_VAL> evict_scores;
  evict_scores.set_attr(attr);
  ObSSMacroCacheType evict_macro_cache_type = ObSSMacroCacheType::MAX_TYPE;
  if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else if (OB_FAIL(get_macro_cache_evict_scores(macro_cache_types, evict_scores))) {
    LOG_WARN("fail to get macro cache evict scores", KR(ret));
  } else if (OB_FAIL(get_evict_macro_cache_type(macro_cache_types, evict_scores, evict_macro_cache_type))) {
    LOG_WARN("fail to get evict macro cache type", KR(ret), K(macro_cache_types), K(evict_scores));
  } else if (OB_FAIL(macro_cache_mgr->get_evict_entries(evict_macro_cache_type, evict_entries))) {
    LOG_WARN("fail to get evict entries", KR(ret), K_SS_MACRO_CACHE_TYPE(evict_macro_cache_type));
  }
  return ret;
}

int ObSSMacroCacheDefaultEvictPolicy::get_macro_cache_evict_scores(
    ObIArray<ObSSMacroCacheType> &macro_cache_types,
    ObIArray<int64_t> &evict_scores)
{
  int ret = OB_SUCCESS;
  int64_t macro_cache_size = 0;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space manager is null", KR(ret));
  } else {
    macro_cache_size = disk_space_mgr->get_unit_config_tnt_macro_cache_size_by_usage_ratio();
  }
  for (uint8_t i = 0; (i < SS_MACRO_CACHE_MAX_TYPE_VAL) && OB_SUCC(ret); ++i) {
    int64_t evict_score = 0;
    ObSSMacroCacheStat macro_cache_stat;
    if (OB_FAIL(disk_space_mgr->get_macro_cache_stat(static_cast<ObSSMacroCacheType>(i), macro_cache_stat))) {
      LOG_WARN("fail to get macro cache stat", KR(ret), K(i));
    } else if (ObSSMacroCacheType::META_FILE == static_cast<ObSSMacroCacheType>(i)) {
      // skip ObSSMacroCacheType::META_FILE, which does not participate in eviction for performance.
    } else if (OB_FAIL(calc_evict_score(macro_cache_size, macro_cache_stat, evict_score))) {
      LOG_WARN("fail to calc evict score", KR(ret), K(macro_cache_size), K(macro_cache_stat));
    } else if (OB_FAIL(macro_cache_types.push_back(static_cast<ObSSMacroCacheType>(i)))) {
      LOG_WARN("fail to push back macro cache type", KR(ret), K(i));
    } else if (OB_FAIL(evict_scores.push_back(evict_score))) {
      LOG_WARN("fail to push back evict score", KR(ret), K(evict_score));
    }
  }
  return ret;
}

int ObSSMacroCacheDefaultEvictPolicy::calc_evict_score(
    const int64_t macro_cache_size,
    const ObSSMacroCacheStat &macro_cache_stat,
    int64_t &evict_score)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(macro_cache_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro cache size", KR(ret), K(macro_cache_size));
  } else if (OB_UNLIKELY(ObSSMacroCacheType::META_FILE == macro_cache_stat.cache_type_)) { // defence
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("META_FILE does not participate in eviction", KR(ret), K(macro_cache_stat));
  } else {
    const int64_t used = macro_cache_stat.used_;
    const int64_t min =  macro_cache_size * macro_cache_stat.get_min() / 100;
    const int64_t weight = macro_cache_size * macro_cache_stat.get_weight() / 100;
    if (OB_UNLIKELY((min <= 0) || (weight <= 0))) { // defence: divide zero exception
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("min and weight should be larger than zero", KR(ret), K(min), K(weight));
    } else if (used <= min) {  // 1. level 1: used <= min
      if (ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == macro_cache_stat.cache_type_) {
        // HOT_TABLET_MACRO_BLOCK does not participate in eviction, when does not exceed min
        evict_score = 0;
      } else {
        // Note: (used * 100 / weight) here means percentage, e.g., 0.02 -> 2 (2%)
        evict_score = used * 100 / weight + LEVEL_1 * macro_cache_size;
      }
    } else {  // 2. level 2: used > min
      // Note: (used * 100 / weight) here means percentage, e.g., 1.1 -> 110 (110%)
      evict_score = used * 100 / weight + LEVEL_2 * macro_cache_size;
    }
  }
  return ret;
}

int ObSSMacroCacheDefaultEvictPolicy::get_evict_macro_cache_type(
    ObIArray<ObSSMacroCacheType> &macro_cache_types,
    ObIArray<int64_t> &evict_scores,
    ObSSMacroCacheType &evict_macro_cache_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(macro_cache_types.empty() ||
                  evict_scores.empty() ||
                  (macro_cache_types.count() != evict_scores.count()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected macro_cache_types count and evict_scores count", KR(ret),
             "macro_cache_type_cnt", macro_cache_types.count(), "evict_score_cnt",
             evict_scores.count());
  } else {
    // find macro_cache_type with max evict_score
    int64_t max_evict_score = 0;
    const int64_t evict_score_count = evict_scores.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < evict_score_count); ++i) {
      const int64_t cur_evict_score = evict_scores.at(i);
      const ObSSMacroCacheType cur_macro_cache_type = macro_cache_types.at(i);
      if (cur_evict_score > max_evict_score) {
        max_evict_score = cur_evict_score;
        evict_macro_cache_type = cur_macro_cache_type;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
