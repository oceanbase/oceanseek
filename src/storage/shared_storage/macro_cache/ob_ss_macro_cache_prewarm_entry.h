/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_PREWARM_ENTRY_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_PREWARM_ENTRY_H_

#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace storage
{

struct ObSSMacroCachePrewarmEntry
{
public:
  ObSSMacroCachePrewarmEntry()
    : effective_tablet_id_(common::ObTabletID::INVALID_TABLET_ID), cache_miss_cnt_(0),
      timestamp_us_(common::ObTimeUtility::current_time_us()), is_prewarmed_(false)
  {}
  explicit ObSSMacroCachePrewarmEntry(const common::ObTabletID &effective_tablet_id)
    : effective_tablet_id_(effective_tablet_id), cache_miss_cnt_(0),
      timestamp_us_(common::ObTimeUtility::current_time_us()), is_prewarmed_(false)
  {}
  virtual ~ObSSMacroCachePrewarmEntry() {}
  ObSSMacroCachePrewarmEntry &operator=(const ObSSMacroCachePrewarmEntry &other)
  {
    if (this != &other) {
      effective_tablet_id_ = other.effective_tablet_id_;
      cache_miss_cnt_ = other.cache_miss_cnt_;
      timestamp_us_ = other.timestamp_us_;
      is_prewarmed_ = other.is_prewarmed_;
    }
    return *this;
  }

  TO_STRING_KV(K_(effective_tablet_id), K_(cache_miss_cnt), K_(timestamp_us), K_(is_prewarmed));

public:
  common::ObTabletID effective_tablet_id_;
  int64_t cache_miss_cnt_;
  int64_t timestamp_us_;
  bool is_prewarmed_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_PREWARM_ENTRY_H_ */
