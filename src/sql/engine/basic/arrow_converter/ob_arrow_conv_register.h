/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 *
 * 转换器分派表声明和 lookup。
 * 未注册的 (OT, AT) 槽位返回 nullptr，由调用方回退到逐行 datum 路径。
 */

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_CONV_REGISTER_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_CONV_REGISTER_H_

#include <arrow/api.h>
#include "sql/engine/basic/arrow_converter/ob_arrow_conv_impl.h"

namespace oceanbase
{
namespace sql
{

constexpr int ARROW_TYPE_MAX = 64;

static_assert(common::ObMaxType < 64,
              "ObMaxType outgrew the LUT; raise ARROW_TYPE_MAX in conv_register.h");
static_assert(static_cast<int>(arrow::Type::MAX_ID) <= 64,
              "arrow::Type::MAX_ID outgrew the LUT; raise ARROW_TYPE_MAX");

extern ArrowConvFunc g_arrow_conv_table[common::ObMaxType][ARROW_TYPE_MAX];

// 越界或未注册都返回 nullptr。
OB_INLINE ArrowConvFunc lookup_conv(common::ObObjType ob_type,
                                    arrow::Type::type arrow_type)
{
  const int arrow_type_int = static_cast<int>(arrow_type);
  if (OB_UNLIKELY(ob_type < 0) || OB_UNLIKELY(ob_type >= common::ObMaxType)
      || OB_UNLIKELY(arrow_type_int < 0) || OB_UNLIKELY(arrow_type_int >= ARROW_TYPE_MAX)) {
    return nullptr;
  }
  return g_arrow_conv_table[ob_type][arrow_type_int];
}

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_CONV_REGISTER_H_
