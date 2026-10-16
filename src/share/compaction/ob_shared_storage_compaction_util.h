// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef OB_SHARE_STORAGE_SHARE_COMPACTION_SHARED_COMPACTION_UTIL_H_
#define OB_SHARE_STORAGE_SHARE_COMPACTION_SHARED_COMPACTION_UTIL_H_
#include "stdint.h"

namespace oceanbase
{
namespace compaction
{
static const int64_t MACRO_STEP_SIZE = 1LL << 32;
static const int64_t INVALID_TASK_IDX = -1;
// atomic protocol/Fail-GC 为每张 SSTable 计入一个 index/meta 序号窗口。
// 该常量只表示窗口数量，不表示 META 的实际起点。
static const int64_t INDEX_TREE_MACRO_SEQ_SLOT_CNT = 1;

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_STORAGE_SHARE_COMPACTION_SHARED_COMPACTION_UTIL_H_
