// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef OB_STORAGE_COMPACTION_MAJOR_PRE_WARMER_H_
#define OB_STORAGE_COMPACTION_MAJOR_PRE_WARMER_H_
#include <stdint.h>
namespace oceanbase
{
namespace compaction
{

enum class ObSSMajorPrewarmLevel : uint8_t
{
  PREWARM_META_AND_DATA_LEVEL = 0,
  PREWARM_ONLY_META_LEVEL = 1,
  PREWARM_NONE_LEVEL = 2,
  PREWARM_MAX_LEVEL = 3
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MAJOR_PRE_WARMER_H_
