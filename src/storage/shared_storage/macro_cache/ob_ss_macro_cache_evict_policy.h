/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_EVICT_POLICY_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_EVICT_POLICY_H_

#include "lib/container/ob_array.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_evict_entry.h"

namespace oceanbase
{
namespace storage
{

// base class for all kinds of evict policy
class ObSSMacroCacheEvictPolicy
{
public:
  ObSSMacroCacheEvictPolicy() {}
  virtual ~ObSSMacroCacheEvictPolicy() {}
  virtual int get_evict_candidates(common::ObIArray<ObSSMacroCacheEvictEntry> &evict_entries) = 0;
};


// prioritize evicting macro cache types that occupy more disk space under the 'min/max/weight' mechanism.
class ObSSMacroCacheDefaultEvictPolicy : public ObSSMacroCacheEvictPolicy
{
public:
  ObSSMacroCacheDefaultEvictPolicy() {}
  virtual ~ObSSMacroCacheDefaultEvictPolicy() {}
  virtual int get_evict_candidates(common::ObIArray<ObSSMacroCacheEvictEntry> &evict_entries) override;

private:
  static const int64_t LEVEL_1 = 1;
  static const int64_t LEVEL_2 = 2;
  int get_macro_cache_evict_scores(common::ObIArray<ObSSMacroCacheType> &macro_cache_types,
                                   common::ObIArray<int64_t> &evict_scores);
  int calc_evict_score(const int64_t macro_cache_size,
                       const ObSSMacroCacheStat &macro_cache_stat,
                       int64_t &evict_score);
  int get_evict_macro_cache_type(common::ObIArray<ObSSMacroCacheType> &macro_cache_types,
                                 common::ObIArray<int64_t> &evict_scores,
                                 ObSSMacroCacheType &evict_macro_cache_type);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_EVICT_POLICY_H_ */
