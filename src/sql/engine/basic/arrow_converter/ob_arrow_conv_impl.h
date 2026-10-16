/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_CONV_IMPL_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_CONV_IMPL_H_

#include <arrow/api.h>
#include "share/vector/ob_i_vector.h"
#include "share/vector/ob_fixed_length_format.h"
#include "share/vector/ob_discrete_format.h"
#include "share/vector/ob_continuous_format.h"
#include "share/vector/ob_uniform_format.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_conv_traits.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_utils.h"

namespace oceanbase
{
namespace sql
{

// 静态分发表槽位的函数类型。builder 先按基类传递，再由实例化后的 functor
// 按 traits 注册类型转成对应的 Builder 子类。
using ArrowConvFunc = int(*)(const ObExpr &expr,
                             common::ObIVector &vec,
                             const ObBitVector &skip,
                             const int64_t size,
                             const bool all_rows_active,
                             arrow::ArrayBuilder &builder,
                             common::ObIAllocator *lob_arena);

template <typename VEC, typename Builder, typename ConvFnObj>
OB_INLINE int batch_convert(const ObExpr &expr,
                            VEC &vec,
                            const ObBitVector &skip,
                            const int64_t size,
                            const bool all_rows_active,
                            Builder &builder,
                            ConvFnObj &conv_fn)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  if (OB_ARROW_FAIL(builder.Reserve(size))) {
    ret = arrow_status_to_ob_ret(status);
    LOG_WARN("arrow builder reserve failed", K(ret), "st", status.ToString().c_str(), K(size));
  } else if (all_rows_active && !vec.has_null()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      ret = conv_fn(expr, i);
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      if (!all_rows_active && skip.at(i)) {
        continue;
      }
      if (vec.is_null(i)) {
        builder.UnsafeAppendNull();
        continue;
      }
      ret = conv_fn(expr, i);
    }
  }
  return ret;
}

template <common::ObObjType OT, arrow::Type::type AT,
          template <typename VEC> class ConvFnTmpl>
int VecConvFormatWrapper(const ObExpr &expr,
                         common::ObIVector &vec,
                         const ObBitVector &skip,
                         const int64_t size,
                         const bool all_rows_active,
                         arrow::ArrayBuilder &builder_base,
                         common::ObIAllocator *lob_arena)
{
  using Tr = ObArrowConvTraits<OT, AT>;
  using Builder = typename Tr::Builder;
  Builder &builder = static_cast<Builder &>(builder_base);

  switch (vec.get_format()) {
    case common::VEC_FIXED: {
      if constexpr (has_ob_cpp_type_v<Tr>) {
        using V = common::ObFixedLengthFormat<typename Tr::ObCppType>;
        V &v = static_cast<V &>(vec);
        ConvFnTmpl<V> fn(expr, v, builder, lob_arena);
        return batch_convert(expr, v, skip, size, all_rows_active, builder, fn);
      } else {
        return OB_ERR_UNEXPECTED;  // 走 FIXED 分支的前提是值能按固定长 C++ 类型直读；没有 ObCppType 说明不该分发到这里
      }
    }
    case common::VEC_DISCRETE: {
      using V = common::ObDiscreteFormat;
      V &v = static_cast<V &>(vec);
      ConvFnTmpl<V> fn(expr, v, builder, lob_arena);
      return batch_convert(expr, v, skip, size, all_rows_active, builder, fn);
    }
    case common::VEC_CONTINUOUS: {
      using V = common::ObContinuousFormat;
      V &v = static_cast<V &>(vec);
      ConvFnTmpl<V> fn(expr, v, builder, lob_arena);
      return batch_convert(expr, v, skip, size, all_rows_active, builder, fn);
    }
    case common::VEC_UNIFORM: {
      using V = common::ObUniformFormat<false>;
      V &v = static_cast<V &>(vec);
      ConvFnTmpl<V> fn(expr, v, builder, lob_arena);
      return batch_convert(expr, v, skip, size, all_rows_active, builder, fn);
    }
    case common::VEC_UNIFORM_CONST: {
      using V = common::ObUniformFormat<true>;
      V &v = static_cast<V &>(vec);
      ConvFnTmpl<V> fn(expr, v, builder, lob_arena);
      return batch_convert(expr, v, skip, size, all_rows_active, builder, fn);
    }
    default:
      return OB_ERR_UNEXPECTED;
  }
}

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_CONV_IMPL_H_
