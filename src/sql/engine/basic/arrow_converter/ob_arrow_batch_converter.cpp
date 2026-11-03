/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/arrow_converter/ob_arrow_batch_converter.h"

#include "lib/allocator/page_arena.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_common.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/udt/ob_array_nested.h"
#include "lib/udt/ob_map_type.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector/ob_continuous_format.h"
#include "share/vector/ob_discrete_format.h"
#include "share/vector/ob_fixed_length_base.h"
#include "share/vector/ob_fixed_length_vector.h"
#include "share/vector/ob_uniform_format.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

// 分派表：命中时走向量化转换，miss 时回退到逐行 datum 路径。
#include "sql/engine/basic/arrow_converter/ob_arrow_conv_register.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_utils.h"

namespace oceanbase
{
namespace sql
{

bool ObArrowBatchConverter::supports_output_type(const common::ObObjType type)
{
  using namespace common;
  switch (type) {
    case ObNullType:
    case ObTinyIntType:
    case ObSmallIntType:
    case ObInt32Type:
    case ObIntType:
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUInt32Type:
    case ObUInt64Type:
    case ObFloatType:
    case ObUFloatType:
    case ObDoubleType:
    case ObUDoubleType:
    case ObDateTimeType:
    case ObTimestampType:
    case ObDateType:
    case ObTimeType:
    case ObYearType:
    case ObNumberType:
    case ObUNumberType:
    case ObDecimalIntType:
    case ObBitType:
    case ObMySQLDateType:
    case ObMySQLDateTimeType:
    case ObVarcharType:
    case ObCharType:
    case ObNVarchar2Type:
    case ObNCharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:
    case ObEnumType:
    case ObSetType:
    case ObURowIDType:
    case ObJsonType:
    case ObLobType:
    case ObCollectionSQLType:
      return true;
    default:
      return false;
  }
}

template <typename CppType, typename Bld>
static int append_fixed_list_elems(arrow::FixedSizeListBuilder *list_bld,
                                   arrow::ArrayBuilder *elem_bld,
                                   const common::ObString &raw,
                                   int32_t dim)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  if (raw.length() >= static_cast<int64_t>(dim * static_cast<int32_t>(sizeof(CppType)))) {
    if (OB_ARROW_FAIL(list_bld->Append())) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append list cell failed", K(ret), "st", status.ToString().c_str());
    }
    const CppType *vals = reinterpret_cast<const CppType *>(raw.ptr());
    auto *eb = static_cast<Bld *>(elem_bld);
    for (int32_t i = 0; OB_SUCC(ret) && i < dim; ++i) {
      if (OB_ARROW_FAIL(eb->Append(vals[i]))) {
        ret = arrow_status_to_ob_ret(status);
        LOG_WARN("append list element failed", K(ret), "st", status.ToString().c_str(), K(i));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("collection payload shorter than declared layout", K(ret), K(raw.length()), K(dim));
  }
  return ret;
}

template <typename CppType, typename Bld>
static int append_var_list_elems(arrow::ListBuilder *list_bld,
                                 arrow::ArrayBuilder *elem_bld,
                                 const common::ObString &raw)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  if (raw.length() < static_cast<int64_t>(sizeof(uint32_t))) {
    ret = OB_ERR_UNEXPECTED;  // 集合列 raw 字节数 < 4（放不下 uint32 元素计数头）
  } else {
    const uint32_t arr_cnt = *reinterpret_cast<const uint32_t *>(raw.ptr());
    const char *body = raw.ptr() + sizeof(uint32_t);
    const int64_t body_len = raw.length() - static_cast<int64_t>(sizeof(uint32_t));
    const int64_t need = static_cast<int64_t>(arr_cnt)
                         * (1 + static_cast<int64_t>(sizeof(CppType)));
    if (body_len < need) {
      ret = OB_ERR_UNEXPECTED;  // 头之后的字节数 < 元素个数 × 每元素至少需要的字节
    } else {
      if (OB_ARROW_FAIL(list_bld->Append())) {
        ret = arrow_status_to_ob_ret(status);
        LOG_WARN("append list cell failed", K(ret), "st", status.ToString().c_str());
      }
      const uint8_t *nulls = reinterpret_cast<const uint8_t *>(body);
      const CppType *vals = reinterpret_cast<const CppType *>(body + arr_cnt);
      for (uint32_t i = 0; OB_SUCC(ret) && i < arr_cnt; ++i) {
        if (nulls[i]) {
          if (OB_ARROW_FAIL(elem_bld->AppendNull())) {
            ret = arrow_status_to_ob_ret(status);
            LOG_WARN("append list null element failed", K(ret), "st", status.ToString().c_str(), K(i));
          }
        } else {
          if (OB_ARROW_FAIL(static_cast<Bld *>(elem_bld)->Append(vals[i]))) {
            ret = arrow_status_to_ob_ret(status);
            LOG_WARN("append list element failed", K(ret), "st", status.ToString().c_str(), K(i));
          }
        }
      }
    }
  }
  return ret;
}

// 按元素类型分发 FIXED_SIZE_LIST 追加逻辑。
static int dispatch_fixed_list_elem(arrow::Type::type elem_tid,
                                    arrow::FixedSizeListBuilder *list_bld,
                                    arrow::ArrayBuilder *elem_bld,
                                    const common::ObString &raw,
                                    int32_t dim)
{
  int ret = OB_SUCCESS;
  switch (elem_tid) {
    case arrow::Type::FLOAT:
      ret = append_fixed_list_elems<float, arrow::FloatBuilder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::DOUBLE:
      ret = append_fixed_list_elems<double, arrow::DoubleBuilder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::INT8:
      ret = append_fixed_list_elems<int8_t, arrow::Int8Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::UINT8:
      ret = append_fixed_list_elems<uint8_t, arrow::UInt8Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::INT16:
      ret = append_fixed_list_elems<int16_t, arrow::Int16Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::UINT16:
      ret = append_fixed_list_elems<uint16_t, arrow::UInt16Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::INT32:
      ret = append_fixed_list_elems<int32_t, arrow::Int32Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::UINT32:
      ret = append_fixed_list_elems<uint32_t, arrow::UInt32Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::INT64:
      ret = append_fixed_list_elems<int64_t, arrow::Int64Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::UINT64:
      ret = append_fixed_list_elems<uint64_t, arrow::UInt64Builder>(list_bld, elem_bld, raw, dim);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

// 按元素类型分发 LIST 追加逻辑。
static int dispatch_var_list_elem(arrow::Type::type elem_tid,
                                  arrow::ListBuilder *list_bld,
                                  arrow::ArrayBuilder *elem_bld,
                                  const common::ObString &raw)
{
  int ret = OB_SUCCESS;
  switch (elem_tid) {
    case arrow::Type::FLOAT:
      ret = append_var_list_elems<float, arrow::FloatBuilder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::DOUBLE:
      ret = append_var_list_elems<double, arrow::DoubleBuilder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::INT8:
      ret = append_var_list_elems<int8_t, arrow::Int8Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::UINT8:
      ret = append_var_list_elems<uint8_t, arrow::UInt8Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::INT16:
      ret = append_var_list_elems<int16_t, arrow::Int16Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::UINT16:
      ret = append_var_list_elems<uint16_t, arrow::UInt16Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::INT32:
      ret = append_var_list_elems<int32_t, arrow::Int32Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::UINT32:
      ret = append_var_list_elems<uint32_t, arrow::UInt32Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::INT64:
      ret = append_var_list_elems<int64_t, arrow::Int64Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::UINT64:
      ret = append_var_list_elems<uint64_t, arrow::UInt64Builder>(list_bld, elem_bld, raw);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

template <typename ArrowBuilder, typename Value>
static int append_native_value_to_builder(arrow::ArrayBuilder &builder,
                                          const arrow::Type::type expected_type,
                                          const Value value)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  if (OB_UNLIKELY(builder.type()->id() != expected_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collection metadata and Arrow builder type differ", K(ret),
             "expected_arrow_type", static_cast<int>(expected_type),
             "actual_arrow_type", static_cast<int>(builder.type()->id()));
  } else if (OB_ARROW_FAIL(static_cast<ArrowBuilder &>(builder).Append(value))) {
    ret = arrow_status_to_ob_ret(status);
    LOG_WARN("append native collection value failed", K(ret),
             "st", status.ToString().c_str());
  }
  return ret;
}

static int append_native_basic_value_to_builder(
    const common::ObObj &value,
    const common::ObCollectionBasicType &basic_type,
    arrow::ArrayBuilder &builder)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  using namespace common;
  if (value.is_null()) {
    if (OB_ARROW_FAIL(builder.AppendNull())) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append null collection element failed", K(ret),
               "st", status.ToString().c_str());
    }
  } else {
    switch (basic_type.basic_meta_.get_obj_type()) {
      case ObTinyIntType:
        ret = append_native_value_to_builder<arrow::Int8Builder>(
            builder, arrow::Type::INT8, value.get_tinyint());
        break;
      case ObSmallIntType:
        ret = append_native_value_to_builder<arrow::Int16Builder>(
            builder, arrow::Type::INT16, value.get_smallint());
        break;
      case ObInt32Type:
        ret = append_native_value_to_builder<arrow::Int32Builder>(
            builder, arrow::Type::INT32, value.get_int32());
        break;
      case ObIntType:
        ret = append_native_value_to_builder<arrow::Int64Builder>(
            builder, arrow::Type::INT64, value.get_int());
        break;
      case ObUTinyIntType:
        ret = append_native_value_to_builder<arrow::UInt8Builder>(
            builder, arrow::Type::UINT8, value.get_utinyint());
        break;
      case ObUSmallIntType:
        ret = append_native_value_to_builder<arrow::UInt16Builder>(
            builder, arrow::Type::UINT16, value.get_usmallint());
        break;
      case ObUInt32Type:
        ret = append_native_value_to_builder<arrow::UInt32Builder>(
            builder, arrow::Type::UINT32, value.get_uint32());
        break;
      case ObUInt64Type:
        ret = append_native_value_to_builder<arrow::UInt64Builder>(
            builder, arrow::Type::UINT64, value.get_uint64());
        break;
      case ObFloatType:
      case ObUFloatType:
        ret = append_native_value_to_builder<arrow::FloatBuilder>(
            builder, arrow::Type::FLOAT, value.get_float());
        break;
      case ObDoubleType:
      case ObUDoubleType:
        ret = append_native_value_to_builder<arrow::DoubleBuilder>(
            builder, arrow::Type::DOUBLE, value.get_double());
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported native nested ARRAY leaf type", K(ret),
                 "element_type", basic_type.basic_meta_.get_obj_type());
        break;
    }
  }
  return ret;
}

static int append_native_basic_array_to_builder(
    const common::ObIArrayType &array,
    const common::ObCollectionBasicType &basic_type,
    arrow::ArrayBuilder &builder)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  for (uint32_t idx = 0; OB_SUCC(ret) && idx < array.size(); ++idx) {
    if (array.is_null(idx)) {
      if (OB_ARROW_FAIL(builder.AppendNull())) {
        ret = arrow_status_to_ob_ret(status);
        LOG_WARN("append null native ARRAY element failed", K(ret),
                 "st", status.ToString().c_str(), K(idx));
      }
    } else {
      common::ObObj value;
      if (OB_FAIL(array.elem_at(idx, value))) {
        LOG_WARN("read native ARRAY element failed", K(ret), K(idx));
      } else if (OB_FAIL(append_native_basic_value_to_builder(value, basic_type, builder))) {
        LOG_WARN("append native ARRAY element failed", K(ret), K(idx));
      }
    }
  }
  return ret;
}

static bool is_nested_array_type(const common::ObCollectionTypeBase *collection_type)
{
  bool is_nested = false;
  if (OB_NOT_NULL(collection_type)
      && collection_type->type_id_ == common::OB_ARRAY_TYPE) {
    const common::ObCollectionArrayType *array_type =
        static_cast<const common::ObCollectionArrayType *>(collection_type);
    is_nested = OB_NOT_NULL(array_type->element_type_)
                && array_type->element_type_->type_id_ == common::OB_ARRAY_TYPE;
  }
  return is_nested;
}

static const common::ObCollectionBasicType *get_map_basic_element_type(
    const common::ObCollectionTypeBase *map_child_type)
{
  const common::ObCollectionBasicType *basic_type = nullptr;
  if (OB_NOT_NULL(map_child_type)
      && map_child_type->type_id_ == common::OB_ARRAY_TYPE) {
    const common::ObCollectionArrayType *array_type =
        static_cast<const common::ObCollectionArrayType *>(map_child_type);
    if (OB_NOT_NULL(array_type->element_type_)
        && array_type->element_type_->type_id_ == common::OB_BASIC_TYPE) {
      basic_type = static_cast<const common::ObCollectionBasicType *>(
          array_type->element_type_);
    }
  }
  return basic_type;
}

static int append_native_map_to_builder(
    const common::ObMapType &map,
    const common::ObCollectionMapType &map_type,
    arrow::ArrayBuilder &builder)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  using namespace common;
  const ObIArrayType *keys = map.get_key_array();
  const ObIArrayType *values = map.get_value_array();
  const ObCollectionBasicType *key_type =
      get_map_basic_element_type(map_type.key_type_);
  const ObCollectionBasicType *value_type =
      get_map_basic_element_type(map_type.value_type_);
  if (builder.type()->id() != arrow::Type::MAP) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("native MAP does not match Arrow builder", K(ret),
             "arrow_type", static_cast<int>(builder.type()->id()));
  } else if (OB_ISNULL(keys) || OB_ISNULL(values)
             || OB_ISNULL(key_type) || OB_ISNULL(value_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("native MAP has invalid key/value metadata", K(ret),
             KP(keys), KP(values), KP(key_type), KP(value_type));
  } else if (map.size() != keys->size() || map.size() != values->size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("native MAP key/value lengths differ", K(ret),
             "map_size", map.size(), "key_size", keys->size(),
             "value_size", values->size());
  } else {
    // Arrow's MapType specifies non-null keys.  Do this scan before appending
    // the map slot so an invalid native map never leaves a partial Arrow value
    // in the builder.  Null values are legal and are appended below.
    for (uint32_t idx = 0; OB_SUCC(ret) && idx < map.size(); ++idx) {
      if (keys->is_null(idx)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("cannot encode native MAP with a null Arrow key", K(ret), K(idx));
      }
    }

    arrow::MapBuilder &map_builder = static_cast<arrow::MapBuilder &>(builder);
    arrow::ArrayBuilder *key_builder = map_builder.key_builder();
    arrow::ArrayBuilder *value_builder = map_builder.item_builder();
    if (OB_SUCC(ret) && (OB_ISNULL(key_builder) || OB_ISNULL(value_builder))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Arrow MAP has null child builder", K(ret), KP(key_builder),
               KP(value_builder));
    } else if (OB_SUCC(ret) && OB_ARROW_FAIL(map_builder.Append())) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append Arrow MAP slot failed", K(ret), "st", status.ToString().c_str());
    }

    for (uint32_t idx = 0; OB_SUCC(ret) && idx < map.size(); ++idx) {
      ObObj key;
      if (OB_FAIL(keys->elem_at(idx, key))) {
        LOG_WARN("read native MAP key failed", K(ret), K(idx));
      } else if (OB_FAIL(append_native_basic_value_to_builder(
                         key, *key_type, *key_builder))) {
        LOG_WARN("append native MAP key failed", K(ret), K(idx));
      } else if (values->is_null(idx)) {
        if (OB_ARROW_FAIL(value_builder->AppendNull())) {
          ret = arrow_status_to_ob_ret(status);
          LOG_WARN("append null native MAP value failed", K(ret),
                   "st", status.ToString().c_str(), K(idx));
        }
      } else {
        ObObj value;
        if (OB_FAIL(values->elem_at(idx, value))) {
          LOG_WARN("read native MAP value failed", K(ret), K(idx));
        } else if (OB_FAIL(append_native_basic_value_to_builder(
                               value, *value_type, *value_builder))) {
          LOG_WARN("append native MAP value failed", K(ret), K(idx));
        }
      }
    }
  }
  return ret;
}

static int append_native_collection_to_builder(
    const common::ObIArrayType &array,
    const common::ObCollectionTypeBase &collection_type,
    arrow::ArrayBuilder &builder,
    common::ObIAllocator &scratch_allocator)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  using namespace common;
  if (collection_type.type_id_ == OB_MAP_TYPE) {
    const ObMapType *map = dynamic_cast<const ObMapType *>(&array);
    if (OB_ISNULL(map)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("native MAP payload has an unexpected object type", K(ret));
    } else {
      ret = append_native_map_to_builder(
          *map, static_cast<const ObCollectionMapType &>(collection_type), builder);
    }
  } else if (collection_type.type_id_ != OB_ARRAY_TYPE
             || builder.type()->id() != arrow::Type::LIST) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported native collection to Arrow shape", K(ret),
             "collection_type", collection_type.type_id_,
             "arrow_type", static_cast<int>(builder.type()->id()));
  } else {
    const ObCollectionArrayType &array_type =
        static_cast<const ObCollectionArrayType &>(collection_type);
    const ObCollectionTypeBase *element_type = array_type.element_type_;
    arrow::ListBuilder &list_builder = static_cast<arrow::ListBuilder &>(builder);
    if (OB_ISNULL(element_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("native ARRAY has null element metadata", K(ret));
    } else if (OB_ARROW_FAIL(list_builder.Append())) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append Arrow LIST slot failed", K(ret),
               "st", status.ToString().c_str());
    } else if (element_type->type_id_ == OB_BASIC_TYPE) {
      ret = append_native_basic_array_to_builder(
          array, static_cast<const ObCollectionBasicType &>(*element_type),
          *list_builder.value_builder());
    } else if (element_type->type_id_ == OB_ARRAY_TYPE) {
      const ObArrayNested *nested_array = dynamic_cast<const ObArrayNested *>(&array);
      arrow::ArrayBuilder *child_builder = list_builder.value_builder();
      if (OB_ISNULL(nested_array) || OB_ISNULL(child_builder)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("native nested ARRAY and Arrow builder differ", K(ret),
                 KP(nested_array), KP(child_builder));
      } else {
        for (uint32_t idx = 0; OB_SUCC(ret) && idx < nested_array->size(); ++idx) {
          if (nested_array->is_null(idx)) {
            if (OB_ARROW_FAIL(child_builder->AppendNull())) {
              ret = arrow_status_to_ob_ret(status);
              LOG_WARN("append null nested ARRAY element failed", K(ret),
                       "st", status.ToString().c_str(), K(idx));
            }
          } else {
            ObIArrayType *child_array = nullptr;
            if (OB_FAIL(ObArrayTypeObjFactory::construct(
                    scratch_allocator, *element_type, child_array, false))) {
              LOG_WARN("construct nested ARRAY child failed", K(ret), K(idx));
            } else if (OB_FAIL(nested_array->at(idx, *child_array))) {
              LOG_WARN("read nested ARRAY child failed", K(ret), K(idx));
            } else if (OB_FAIL(append_native_collection_to_builder(
                           *child_array, *element_type, *child_builder,
                           scratch_allocator))) {
              LOG_WARN("append nested ARRAY child failed", K(ret), K(idx));
            }
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported native ARRAY element metadata", K(ret),
               "element_type", element_type->type_id_);
    }
  }
  return ret;
}

static int resolve_collection_meta(const ObExpr &expr,
                                   ObEvalCtx &eval_ctx,
                                   const common::ObCollectionTypeBase *&collection_type)
{
  int ret = OB_SUCCESS;
  collection_type = nullptr;
  const uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  common::ObCollectionTypeBase *resolved_type = nullptr;
  if (UINT_MAX16 == subschema_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collection expression has no subschema id", K(ret), K(expr.obj_meta_));
  } else if (OB_FAIL(ObArrayExprUtils::get_coll_type_by_subschema_id(
                 &eval_ctx.exec_ctx_, subschema_id, resolved_type))) {
    LOG_WARN("resolve collection subschema failed", K(ret), K(subschema_id));
  } else if (OB_ISNULL(resolved_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collection subschema has null metadata", K(ret), K(subschema_id));
  } else {
    collection_type = resolved_type;
  }
  return ret;
}

static int append_collection_raw_to_builder(const common::ObString &raw,
                                            arrow::ArrayBuilder &builder)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  if (OB_ISNULL(raw.ptr())) {
    if (OB_ARROW_FAIL(builder.AppendNull())) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append null failed", K(ret), "st", status.ToString().c_str());
    }
  } else {
    const arrow::Type::type tid = builder.type()->id();
    if (tid == arrow::Type::FIXED_SIZE_LIST) {
      auto *list_bld = static_cast<arrow::FixedSizeListBuilder *>(&builder);
      const std::shared_ptr<arrow::DataType> type_sptr = builder.type();
      const arrow::FixedSizeListType *list_type =
          static_cast<const arrow::FixedSizeListType *>(type_sptr.get());
      const int32_t dim = list_type->list_size();
      const arrow::Type::type elem_tid = list_type->value_type()->id();
      arrow::ArrayBuilder *elem_bld = list_bld->value_builder();
      if (OB_FAIL(dispatch_fixed_list_elem(elem_tid, list_bld, elem_bld, raw, dim))) {
        LOG_WARN("append fixed-size-list failed", K(ret), K(elem_tid), K(dim), K(raw.length()));
      }
    } else if (tid == arrow::Type::LIST) {
      auto *list_bld = static_cast<arrow::ListBuilder *>(&builder);
      const std::shared_ptr<arrow::DataType> type_sptr = builder.type();
      const arrow::ListType *list_type = static_cast<const arrow::ListType *>(type_sptr.get());
      const arrow::Type::type elem_tid = list_type->value_type()->id();
      arrow::ArrayBuilder *elem_bld = list_bld->value_builder();
      if (OB_FAIL(dispatch_var_list_elem(elem_tid, list_bld, elem_bld, raw))) {
        LOG_WARN("append list failed", K(ret), K(elem_tid), K(raw.length()));
      }
    } else if (tid == arrow::Type::LARGE_BINARY) {
      if (OB_ARROW_FAIL(static_cast<arrow::LargeBinaryBuilder &>(builder).Append(
              reinterpret_cast<const uint8_t *>(raw.ptr()), static_cast<int64_t>(raw.length())))) {
        ret = arrow_status_to_ob_ret(status);
        LOG_WARN("append large binary failed", K(ret), "st", status.ToString().c_str());
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported collection builder type", K(ret), K(tid));
    }
  }
  return ret;
}

static int append_collection_datum_to_builder(const ObExpr &expr,
                                              const common::ObDatum &datum,
                                              arrow::ArrayBuilder &builder,
                                              common::ObIAllocator &arena,
                                              ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  const common::ObCollectionTypeBase *collection_type = nullptr;
  if (OB_NOT_NULL(eval_ctx)
      && (builder.type()->id() == arrow::Type::LIST
          || builder.type()->id() == arrow::Type::MAP)
      && OB_FAIL(resolve_collection_meta(expr, *eval_ctx, collection_type))) {
    LOG_WARN("resolve collection metadata failed", K(ret), K(expr.obj_meta_));
  } else if (OB_NOT_NULL(collection_type)
             && ((builder.type()->id() == arrow::Type::LIST
                  && is_nested_array_type(collection_type))
                 || (builder.type()->id() == arrow::Type::MAP
                     && collection_type->type_id_ == common::OB_MAP_TYPE))) {
    common::ObIArrayType *array = nullptr;
    const uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
    // Arrow builders copy the values below, so parsing and walking one native
    // nested collection needs only a short-lived workspace.  Keeping it out of
    // lob_arena avoids retaining a full native object tree for every row in a
    // Flight record batch.
    common::ObArenaAllocator collection_arena(
        "FlSqlCollRead", common::OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    if (OB_FAIL(ObArrayExprUtils::get_array_obj(
            collection_arena, *eval_ctx, subschema_id, datum.get_string(), array))) {
      LOG_WARN("construct native collection from collection payload failed", K(ret),
               K(subschema_id));
    } else if (OB_FAIL(append_native_collection_to_builder(
                   *array, *collection_type, builder, collection_arena))) {
      LOG_WARN("append native collection to Arrow builder failed", K(ret));
    }
  } else {
    // Keep the existing one-level raw fast path for legacy direct callers and
    // for non-nested ARRAY/VECTOR results.  Nested values must use the native
    // parser above; their payload has offsets and child serialization, not the
    // flat [count][null bitmap][primitive values] layout.
    common::ObString raw;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(
            arena, datum, expr.datum_meta_, raw))) {
      LOG_WARN("read collection payload failed", K(ret), K(expr.datum_meta_.type_));
    } else {
      ret = append_collection_raw_to_builder(raw, builder);
    }
  }
  return ret;
}

static int append_string_datum_to_builder(const ObExpr &expr,
                                          const common::ObDatum &datum,
                                          arrow::ArrayBuilder &builder,
                                          common::ObIAllocator &arena)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  common::ObString s;
  if (OB_FAIL(ObTextStringHelper::read_real_string_data(
          arena, datum, expr.datum_meta_, s))) {
    LOG_WARN("read string payload failed", K(ret), K(expr.datum_meta_.type_));
  } else if (builder.type()->id() == arrow::Type::LARGE_BINARY) {
    // The caller checked datum.is_null(); a null pointer with length zero is
    // a valid empty binary value, which Arrow Append preserves as non-null.
    if (OB_ARROW_FAIL(static_cast<arrow::LargeBinaryBuilder &>(builder).Append(
            reinterpret_cast<const uint8_t *>(s.ptr()), static_cast<int64_t>(s.length())))) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append binary failed", K(ret), "st", status.ToString().c_str());
    }
  } else if (OB_ARROW_FAIL(static_cast<arrow::StringBuilder &>(builder).Append(
                 s.ptr(), static_cast<int32_t>(s.length())))) {
    ret = arrow_status_to_ob_ret(status);
    LOG_WARN("append string failed", K(ret), "st", status.ToString().c_str());
  }
  return ret;
}

static int append_urowid_datum_to_builder(const common::ObDatum &datum,
                                          arrow::ArrayBuilder &builder,
                                          common::ObIAllocator &arena)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  const common::ObURowIDData rowid = datum.get_urowid();
  const int64_t buf_size = rowid.needed_base64_buffer_size();
  char *buf = nullptr;
  int64_t pos = 0;
  if (OB_UNLIKELY(builder.type()->id() != arrow::Type::STRING)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("urowid requires Arrow string builder", K(ret),
             "arrow_type", static_cast<int>(builder.type()->id()));
  } else if (OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid urowid base64 buffer size", K(ret), K(buf_size));
  } else if (OB_ISNULL(buf = static_cast<char *>(arena.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate urowid base64 buffer failed", K(ret), K(buf_size));
  } else if (OB_FAIL(rowid.get_base64_str(buf, buf_size, pos))) {
    LOG_WARN("encode urowid as base64 failed", K(ret), K(buf_size));
  } else if (OB_ARROW_FAIL(static_cast<arrow::StringBuilder &>(builder).Append(
                 buf, static_cast<int32_t>(pos)))) {
    ret = arrow_status_to_ob_ret(status);
    LOG_WARN("append urowid string failed", K(ret), "st", status.ToString().c_str());
  }
  return ret;
}

static int append_json_datum_to_builder(const ObExpr &expr,
                                        const common::ObDatum &datum,
                                        arrow::ArrayBuilder &builder,
                                        common::ObIAllocator &arena)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  common::ObString raw;
  if (OB_FAIL(ObTextStringHelper::read_real_string_data(
          arena, datum, expr.datum_meta_, raw))) {
    LOG_WARN("read json payload failed", K(ret));
  } else if (OB_ISNULL(raw.ptr()) || raw.length() == 0) {
    if (OB_ARROW_FAIL(builder.AppendNull())) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append null failed", K(ret), "st", status.ToString().c_str());
    }
  } else {
    common::ObJsonBin j_bin(raw.ptr(), raw.length(), &arena);
    common::ObJsonBuffer jbuf(&arena);
    j_bin.set_seek_flag(true);
    if (OB_SUCCESS != j_bin.reset_iter()
        || OB_SUCCESS != j_bin.print(jbuf, true, raw.length())
        || jbuf.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("json bin print failed", K(ret));
    } else if (OB_ARROW_FAIL(static_cast<arrow::StringBuilder &>(builder).Append(
                   jbuf.ptr(), static_cast<int32_t>(jbuf.length())))) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append json failed", K(ret), "st", status.ToString().c_str());
    }
  }
  return ret;
}

static void arrow_get_decimal_meta(arrow::ArrayBuilder &builder,
                                   int32_t &arrow_prec,
                                   int32_t &arrow_scale,
                                   int &arrow_type_id)
{
  arrow_prec = -1;
  arrow_scale = -1;
  arrow_type_id = -1;
  if (OB_ISNULL(builder.type())) {
    return;
  }
  arrow_type_id = static_cast<int>(builder.type()->id());
  if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL128)) {
    const arrow::Decimal128Type *t =
        static_cast<const arrow::Decimal128Type *>(builder.type().get());
    arrow_prec = t->precision();
    arrow_scale = t->scale();
  } else if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL256)) {
    const arrow::Decimal256Type *t =
        static_cast<const arrow::Decimal256Type *>(builder.type().get());
    arrow_prec = t->precision();
    arrow_scale = t->scale();
  }
}

template <typename ArrowDecimalType, typename ArrowBuilderType>
static int append_scaled_decimal_int_to_arrow(ArrowBuilderType &builder,
                                              const common::ObDecimalInt *decint,
                                              const int32_t int_bytes)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  const int32_t arrow_bytes = static_cast<int32_t>(ArrowDecimalType::kByteWidth);
  common::ObDecimalIntBuilder extended;
  extended.from(decint, int_bytes);
  if (extended.get_int_bytes() < arrow_bytes) {
    extended.extend(arrow_bytes);
  }
  if (extended.get_int_bytes() > arrow_bytes) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("decimal width too large", K(ret), "bytes", extended.get_int_bytes(), K(arrow_bytes));
  } else {
    const int32_t final_bytes = extended.get_int_bytes();
    const uint8_t *buffer = reinterpret_cast<const uint8_t *>(extended.get_decimal_int());
    uint8_t buf[ArrowDecimalType::kByteWidth] = {0};
    for (int32_t i = 0; i < final_bytes; ++i) {
      buf[arrow_bytes - 1 - i] = buffer[i];
    }
    const arrow::Result<ArrowDecimalType> dec_res =
        ArrowDecimalType::FromBigEndian(buf, arrow_bytes);
    if (!dec_res.ok()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("FromBigEndian failed", K(ret), "st", dec_res.status().ToString().c_str());
    } else if (OB_ARROW_FAIL(builder.Append(dec_res.ValueOrDie()))) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append decimal failed", K(ret), "st", status.ToString().c_str());
    }
  }
  return ret;
}

static int append_decimal_datum_to_builder(const ObExpr &expr,
                                           const common::ObDatum &datum,
                                           arrow::ArrayBuilder &builder)
{
  int ret = OB_SUCCESS;
  using namespace common;
  int32_t tgt_prec = -1;
  int32_t tgt_scale = -1;
  int arrow_type_id = -1;
  arrow_get_decimal_meta(builder, tgt_prec, tgt_scale, arrow_type_id);
  if (tgt_prec <= 0 || tgt_scale < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid target decimal meta", K(ret), K(tgt_prec), K(tgt_scale));
  } else {
    ObDecimalIntBuilder work;
    ObDecimalIntBuilder scaled;
    const ObDecimalInt *src_decint = nullptr;
    int32_t src_int_bytes = 0;
    ObScale src_scale = 0;

    if (ObNumberType == expr.datum_meta_.type_ || ObUNumberType == expr.datum_meta_.type_) {
      const number::ObNumber &nmb = datum.get_number();
      src_scale = nmb.get_scale();
      ObDecimalInt *tmp_decint = nullptr;
      int32_t tmp_int_bytes = 0;
      if (OB_FAIL(wide::from_number(nmb, work, src_scale, tmp_decint, tmp_int_bytes))) {
        LOG_WARN("from_number failed", K(ret));
      } else {
        src_decint = tmp_decint;
        src_int_bytes = tmp_int_bytes;
      }
    } else if (ObDecimalIntType == expr.datum_meta_.type_) {
      src_decint = datum.get_decimal_int();
      src_int_bytes = static_cast<int32_t>(datum.len_);
      src_scale = static_cast<ObScale>(expr.datum_meta_.scale_);
      if (OB_ISNULL(src_decint)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null decimal int payload", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unsupported decimal source type", K(ret), K(expr.datum_meta_.type_));
    }

    if (OB_SUCC(ret)) {
      const ObPrecision out_prec = static_cast<ObPrecision>(tgt_prec);
      const ObScale out_scale = static_cast<ObScale>(tgt_scale);
      const int32_t out_int_bytes =
          wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
      const ObDecimalInt *out_decint = src_decint;
      int32_t out_bytes = src_int_bytes;

      if (sql::ObDatumCast::need_scale_decimalint(src_scale, src_int_bytes, out_scale,
                                                  out_int_bytes)) {
        if (OB_FAIL(sql::ObDatumCast::common_scale_decimalint(
                src_decint, src_int_bytes, src_scale, out_scale, out_prec, CM_NONE, scaled))) {
          LOG_WARN("common_scale_decimalint failed", K(ret));
        } else {
          out_decint = scaled.get_decimal_int();
          out_bytes = scaled.get_int_bytes();
        }
      }

      if (OB_SUCC(ret)) {
        if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL128)) {
          ret = append_scaled_decimal_int_to_arrow<arrow::Decimal128, arrow::Decimal128Builder>(
              static_cast<arrow::Decimal128Builder &>(builder), out_decint, out_bytes);
        } else if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL256)) {
          ret = append_scaled_decimal_int_to_arrow<arrow::Decimal256, arrow::Decimal256Builder>(
              static_cast<arrow::Decimal256Builder &>(builder), out_decint, out_bytes);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected arrow decimal type id", K(ret), K(arrow_type_id));
        }
      }
    }
  }
  return ret;
}

template <typename VecRef>
static int append_vec_by_datum(const ObExpr &expr, VecRef &vec,
                               const ObBitVector &skip, const int64_t size,
                               ObEvalCtx &eval_ctx,
                               arrow::ArrayBuilder &builder,
                               common::ObIAllocator &arena)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  bool is_null = false;
  const char *payload = nullptr;
  common::ObLength length = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    if (skip.at(i)) {
      continue;
    }
    vec.get_payload(i, is_null, payload, length);
    if (is_null) {
      if (OB_ARROW_FAIL(builder.AppendNull())) {
        ret = arrow_status_to_ob_ret(status);
        LOG_WARN("append null failed", K(ret), "st", status.ToString().c_str(), K(i));
      }
    } else {
      const common::ObDatum datum(payload, static_cast<uint32_t>(length), false);
      if (OB_FAIL(ObArrowBatchConverter::append_datum_to_builder(
              expr, datum, builder, arena, &eval_ctx))) {
        LOG_WARN("append datum to builder failed", K(ret), K(i));
      }
    }
  }
  return ret;
}


int ObArrowBatchConverter::append_datum_to_builder(const ObExpr &expr,
                                                   const ObDatum &datum,
                                                   arrow::ArrayBuilder &builder,
                                                   common::ObIAllocator &lob_arena,
                                                   ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  using namespace common;
  if (datum.is_null()) {
    ret = arrow_status_to_ob_ret(builder.AppendNull());
  } else {
    switch (expr.datum_meta_.type_) {
      case ObNullType:
        ret = arrow_status_to_ob_ret(builder.AppendNull());
        break;
      case ObTinyIntType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::Int8Builder&>(builder).Append(
            datum.get_int8()));
        break;
      case ObSmallIntType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::Int16Builder&>(builder).Append(
            static_cast<int16_t>(datum.get_int())));
        break;
      case ObInt32Type:
        ret = arrow_status_to_ob_ret(static_cast<arrow::Int32Builder&>(builder).Append(
            static_cast<int32_t>(datum.get_int())));
        break;
      case ObIntType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::Int64Builder&>(builder).Append(
            datum.get_int()));
        break;
      case ObUTinyIntType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::UInt8Builder&>(builder).Append(
            static_cast<uint8_t>(datum.get_uint())));
        break;
      case ObUSmallIntType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::UInt16Builder&>(builder).Append(
            static_cast<uint16_t>(datum.get_uint())));
        break;
      case ObUInt32Type:
        ret = arrow_status_to_ob_ret(static_cast<arrow::UInt32Builder&>(builder).Append(
            static_cast<uint32_t>(datum.get_uint())));
        break;
      case ObUInt64Type:
        ret = arrow_status_to_ob_ret(static_cast<arrow::UInt64Builder&>(builder).Append(
            datum.get_uint()));
        break;
      case ObFloatType:
      case ObUFloatType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::FloatBuilder&>(builder).Append(
            datum.get_float()));
        break;
      case ObDoubleType:
      case ObUDoubleType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::DoubleBuilder&>(builder).Append(
            datum.get_double()));
        break;
      case ObDateTimeType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::TimestampBuilder&>(builder).Append(
            datum.get_int()));
        break;
      case ObTimestampType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::TimestampBuilder&>(builder).Append(
            datum.get_int()));
        break;
      case ObDateType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::Date32Builder&>(builder).Append(
            datum.get_date()));
        break;
      case ObTimeType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::DurationBuilder&>(builder).Append(
            datum.get_time()));
        break;
      case ObYearType: {
        const uint8_t raw = datum.get_year();
        const int16_t year_val = (raw == 0) ? 0 : static_cast<int16_t>(raw + 1900);
        ret = arrow_status_to_ob_ret(static_cast<arrow::Int16Builder&>(builder).Append(year_val));
        break;
      }
      case ObNumberType:
      case ObUNumberType:
      case ObDecimalIntType:
        ret = append_decimal_datum_to_builder(expr, datum, builder);
        break;
      case ObBitType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::UInt64Builder&>(builder).Append(
            datum.get_bit()));
        break;
      case ObMySQLDateType: {
        const ObMySQLDate md = datum.get_mysql_date();
        int32_t d_value = 0;
        if (OB_SUCCESS != common::ObTimeConverter::mdate_to_date(md, d_value, 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mdate_to_date failed", K(ret));
        } else {
          ret = arrow_status_to_ob_ret(static_cast<arrow::Date32Builder&>(builder).Append(d_value));
        }
        break;
      }
      case ObMySQLDateTimeType: {
        const ObMySQLDateTime mdt = datum.get_mysql_datetime();
        int64_t dt_value = 0;
        if (OB_SUCCESS != common::ObTimeConverter::mdatetime_to_datetime(mdt, dt_value, 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mdatetime_to_datetime failed", K(ret));
        } else {
          ret = arrow_status_to_ob_ret(
              static_cast<arrow::TimestampBuilder&>(builder).Append(dt_value));
        }
        break;
      }
      case ObVarcharType:
      case ObCharType:
      case ObNVarchar2Type:
      case ObNCharType:
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType:
      case ObEnumType:
      case ObSetType:
      case ObLobType:
        ret = append_string_datum_to_builder(expr, datum, builder, lob_arena);
        break;
      case ObURowIDType:
        ret = append_urowid_datum_to_builder(datum, builder, lob_arena);
        break;
      case ObJsonType:
        ret = append_json_datum_to_builder(expr, datum, builder, lob_arena);
        break;
      case ObCollectionSQLType:
        ret = append_collection_datum_to_builder(
            expr, datum, builder, lob_arena, eval_ctx);
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported ob type for arrow conversion", K(ret),
                 "ob_type", common::ob_obj_type_str(expr.datum_meta_.type_));
        break;
    }
  }
  return ret;
}

int ObArrowBatchConverter::append_brs_to_builders(
    const ExprFixedArray &output_exprs,
    ObEvalCtx &eval_ctx,
    const ObBatchRows &brs,
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> &builders,
    common::ObIAllocator &lob_arena)
{
  int ret = OB_SUCCESS;
  const int64_t ncols = output_exprs.count();
  const int64_t nrows = brs.size_;
  if (OB_UNLIKELY(static_cast<int64_t>(builders.size()) != ncols)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("builder count mismatch", K(ret), "builders", builders.size(), K(ncols));
    // The callers construct builders from the result schema and expressions
    // from the physical operator.  They must remain one-to-one.  Do not
    // attempt conversion when that invariant has been broken: Arrow builders
    // use unchecked append paths internally, so continuing can corrupt memory.
  } else if (OB_SUCC(ret)) {
    for (int64_t col = 0; OB_SUCC(ret) && col < ncols; ++col) {
      ObExpr *expr = output_exprs.at(col);
      common::ObIVector *vec = nullptr;
      if (OB_ISNULL(expr) || OB_ISNULL(builders[col])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr or builder", K(ret), K(col));
      } else if (UINT32_MAX == expr->vector_header_off_) {
        // Legacy batch expressions have datum arrays without vector storage.
        // Calling eval_vector/get_vector would read an invalid frame offset.
        ObEvalCtx::BatchInfoScopeGuard batch_guard(eval_ctx);
        batch_guard.set_batch_size(nrows);
        batch_guard.set_batch_idx(0);
        if (OB_FAIL(expr->eval_batch(eval_ctx, *brs.skip_, nrows))) {
          LOG_WARN("eval_batch failed", K(ret), K(col));
        } else {
          const common::ObDatum *datums = expr->locate_batch_datums(eval_ctx);
          for (int64_t row = 0; OB_SUCC(ret) && row < nrows; ++row) {
            if (brs.all_rows_active_ || !brs.skip_->at(row)) {
              batch_guard.set_batch_idx(row);
              const common::ObDatum &datum = datums[expr->is_batch_result() ? row : 0];
              if (OB_FAIL(append_datum_to_builder(
                      *expr, datum, *builders[col], lob_arena, &eval_ctx))) {
                LOG_WARN("append batch datum failed", K(ret), K(col), K(row));
              }
            }
          }
        }
      } else if (OB_FAIL(expr->eval_vector(eval_ctx, brs))) {
        LOG_WARN("eval_vector failed", K(ret), K(col));
      } else if (OB_ISNULL(vec = expr->get_vector(eval_ctx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_vector failed", K(ret), K(col));
      } else {
        const common::ObObjType ob_t = expr->datum_meta_.type_;
        const arrow::Type::type ar_t = builders[col]->type()->id();
        // 从静态分发表中获取处理函数。
        ArrowConvFunc fn = lookup_conv(ob_t, ar_t);

        if (fn != nullptr) {
          // 命中静态分发表时走向量化路径。
          if (OB_FAIL(fn(*expr, *vec, *brs.skip_, nrows, brs.all_rows_active_,
                         *builders[col], &lob_arena))) {
            LOG_WARN("arrow converter fn failed — NOT retrying datum path; check LUT/group invariant",
                     K(ret), K(col), K(ob_t), K(ar_t));
          }
        } else {
          // 当前 (ob_t, ar_t) 未注册时回退到 datum 路径。
          if (OB_FAIL(append_vec_by_datum(*expr, *vec, *brs.skip_, nrows,
                                          eval_ctx, *builders[col], lob_arena))) {
            LOG_WARN("datum fallback failed", K(ret), K(col));
          }
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
