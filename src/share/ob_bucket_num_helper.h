/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_SHARED_STORAGE_SHARE_OB_BUCKET_NUM_HELPER_H_
#define OCEANBASE_SHARED_STORAGE_SHARE_OB_BUCKET_NUM_HELPER_H_

#include <stdint.h>
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace share
{

enum class ObGetBucketNumType : uint8_t
{
  TABLET_TYPE = 0,
  MACRO_TYPE = 1
};

class ObBucketNumHelper
{
public:
  static int64_t get_bucket_num_by_mtl_mem(const ObGetBucketNumType type)
  {
    int64_t bucket_num = 0;
    const int64_t GB = 1 * 1024L * 1024L * 1024L;
    const int64_t tenant_mem = MTL_MEM_SIZE();
    if ((tenant_mem / GB) <= 4) { // tenant_mem <= 4GB
      if (ObGetBucketNumType::TABLET_TYPE == type) {
        bucket_num = 10000; // almost 1w buckets. actually cal_next_prime return 1.2w buckets
      } else if (ObGetBucketNumType::MACRO_TYPE == type) {
        bucket_num = 90000; // almost 10w buckets. actually cal_next_prime return 9.8w buckets
      }
    } else { // tenant_mem > 4GB
      if (ObGetBucketNumType::TABLET_TYPE == type) {
        bucket_num = 90000; // almost 10w buckets. actually cal_next_prime return 9.8w buckets
      } else if (ObGetBucketNumType::MACRO_TYPE == type) {
        bucket_num = 1000000; // almost 100w buckets. actually cal_next_prime return 157w buckets
      }
    }
    return bucket_num;
  }
};

} /* namespace share */
} /* namespace oceanbase */

#endif /* OCEANBASE_SHARED_STORAGE_SHARE_OB_BUCKET_NUM_HELPER_H_ */
