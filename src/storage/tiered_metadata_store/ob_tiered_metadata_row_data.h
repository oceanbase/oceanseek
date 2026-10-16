/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_TIERED_METADATA_ROW_DATA_H_
#define OCEANBASE_SHARED_STORAGE_TIERED_METADATA_ROW_DATA_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace storage
{

// __all_shared_tiered_meta 中一条逻辑行的非 rowkey 列。
// meta_type_ 标识元数据类型（正常取值见 ObSSTieredMetaTypeEnum），这里用 int64_t 保存，
// 不做枚举校验：备份恢复场景可能带未知值，读取路径需要原样透传。
// 一条逻辑行可能由多个物理 part 组成，返回该结构前会先重组 meta_value_。
// 反序列化后，meta_value_ 指向输入缓冲区。
struct ObTieredMetaTableRowData final
{
  OB_UNIS_VERSION_V(1);
public:
  ObTieredMetaTableRowData()
    : meta_type_(-1),
      meta_value_()
  {}
  ~ObTieredMetaTableRowData() = default;

  void reset()
  {
    meta_type_ = -1;
    meta_value_.reset();
  }
  // meta_type_ 允许无效值，is_valid() 只检查 meta_value_ 是否为空。
  bool is_valid() const;
  TO_STRING_KV(K_(meta_type), K_(meta_value));

  int64_t meta_type_;
  common::ObString meta_value_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_SHARED_STORAGE_TIERED_METADATA_ROW_DATA_H_
