/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/logical_table/ob_logical_table_encoder.h"

#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/logical_table/ob_logical_table_codec_utils.h"
#include "share/logical_table/ob_namespace_ru_manager.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
namespace share
{

int ObLogicalTableEncoder::encode_key(
  const ObLogicalTableKeyPrefix &prefix,
  const common::ObIArray<common::ObDatum> &datums,
  common::ObIAllocator &allocator,
  common::ObString &key)
{
  int ret = OB_SUCCESS;
  int64_t encoded_len = 0;
  int64_t pos = 0;
  char *buf = nullptr;
  key.reset();

  if (OB_UNLIKELY(!prefix.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical table key prefix", K(ret), K(prefix));
  } else if (OB_FAIL(calc_encoded_key_len_(prefix.aux_type_, datums, encoded_len))) {
    LOG_WARN("failed to calculate encoded key length", K(ret), K(prefix), K(datums.count()));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(encoded_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate key buffer", K(ret), K(encoded_len));
  } else if (OB_FAIL(encode_key_to_buf_(prefix, datums, buf, encoded_len, pos))) {
    LOG_WARN("failed to encode logical table key", K(ret), K(prefix), K(encoded_len));
  } else {
    key.assign_ptr(buf, static_cast<int32_t>(pos));
    try_consume_data_(prefix.namespace_id_, pos);
  }

  return ret;
}

int ObLogicalTableEncoder::encode_value(
  const common::ObIArray<common::ObString> &column_names,
  const common::ObIArray<common::ObObjMeta> &metas,
  const common::ObIArray<common::ObDatum> &datums,
  common::ObIAllocator &allocator,
  common::ObString &value_binary,
  const uint64_t namespace_id)
{
  int ret = OB_SUCCESS;
  common::ObJsonObject root(&allocator);
  value_binary.reset();

  if (OB_UNLIKELY(column_names.count() != metas.count() || metas.count() != datums.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("logical table value input size mismatch",
      K(ret), K(column_names.count()), K(metas.count()), K(datums.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
      const common::ObString &column_name = column_names.at(i);
      const common::ObObjMeta &meta = metas.at(i);
      const common::ObDatum &datum = datums.at(i);
      common::ObJsonNode *value_node = nullptr;

      if (OB_UNLIKELY(column_name.empty())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid column name", K(ret), K(i), K(column_name));
      } else if (OB_NOT_NULL(root.get_value(column_name))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("duplicated column name in logical table value", K(ret), K(i), K(column_name));
      } else if (OB_FAIL(build_json_value_node_(meta, datum, allocator, value_node))) {
        LOG_WARN("failed to build json value node", K(ret), K(i), K(column_name), K(meta));
      } else if (OB_ISNULL(value_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("json value node is null", K(ret), K(i), K(column_name));
      } else if (OB_FAIL(root.add(column_name, value_node))) {
        LOG_WARN("failed to append value node", K(ret), K(i), K(column_name));
      }
    }
  }

  if (OB_SUCC(ret)) {
    common::ObIJsonBase *json_base = &root;
    if (OB_FAIL(json_base->get_raw_binary(value_binary, &allocator))) {
      LOG_WARN("failed to serialize logical table value", K(ret));
    } else {
      try_consume_data_(namespace_id, value_binary.length());
    }
  }

  return ret;
}

int ObLogicalTableEncoder::encode_value(
  const common::ObIArray<common::ObString> &column_names,
  const common::ObIArray<common::ObString> &json_paths,
  const common::ObIArray<common::ObObjMeta> &metas,
  const common::ObIArray<common::ObDatum> &datums,
  common::ObIAllocator &allocator,
  common::ObString &value_binary,
  const uint64_t namespace_id)
{
  int ret = OB_SUCCESS;
  common::ObJsonObject root(&allocator);
  value_binary.reset();

  if (OB_UNLIKELY(column_names.count() != json_paths.count()
      || json_paths.count() != metas.count()
      || metas.count() != datums.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("logical table value path input size mismatch",
             K(ret), K(column_names.count()), K(json_paths.count()),
             K(metas.count()), K(datums.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
      const common::ObString &column_name = column_names.at(i);
      const common::ObString &json_path = json_paths.at(i);
      const common::ObObjMeta &meta = metas.at(i);
      const common::ObDatum &datum = datums.at(i);
      common::ObJsonNode *value_node = nullptr;
      if (json_path.empty()) {
        if (OB_UNLIKELY(column_name.empty())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid column name", K(ret), K(i), K(column_name));
        } else if (OB_NOT_NULL(root.get_value(column_name))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("duplicated column name in logical table value", K(ret), K(i), K(column_name));
        } else if (OB_FAIL(build_json_value_node_(meta, datum, allocator, value_node))) {
          LOG_WARN("failed to build json value node", K(ret), K(i), K(column_name), K(meta));
        } else if (OB_ISNULL(value_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("json value node is null", K(ret), K(i), K(column_name));
        } else if (OB_FAIL(root.add(column_name, value_node))) {
          LOG_WARN("failed to append value node", K(ret), K(i), K(column_name));
        }
      } else if (OB_FAIL(append_json_member_path_value_(root, json_path, meta, datum, allocator))) {
        LOG_WARN("failed to append json path value", K(ret), K(i), K(json_path), K(meta));
      }
    }
  }

  if (OB_SUCC(ret)) {
    common::ObIJsonBase *json_base = &root;
    if (OB_FAIL(json_base->get_raw_binary(value_binary, &allocator))) {
      LOG_WARN("failed to serialize logical table value", K(ret));
    } else {
      try_consume_data_(namespace_id, value_binary.length());
    }
  }

  return ret;
}

int ObLogicalTableEncoder::append_json_member_path_value_(
  common::ObJsonObject &root,
  const common::ObString &json_path,
  const common::ObObjMeta &meta,
  const common::ObDatum &datum,
  common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  common::ObJsonObject *cur_obj = &root;
  common::ObJsonNode *value_node = nullptr;
  if (OB_UNLIKELY(json_path.length() < 3
      || json_path.ptr()[0] != '$'
      || json_path.ptr()[1] != '.')) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only json member path is supported", K(ret), K(json_path));
  } else {
    int64_t seg_start = 2;
    while (OB_SUCC(ret) && seg_start < json_path.length()) {
      int64_t seg_end = seg_start;
      while (seg_end < json_path.length() && json_path.ptr()[seg_end] != '.') {
        if (json_path.ptr()[seg_end] == '[' || json_path.ptr()[seg_end] == ']') {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("array json path is not supported", K(ret), K(json_path));
        }
        ++seg_end;
      }
      if (OB_FAIL(ret)) {
      } else if (seg_end == seg_start) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("empty json path segment", K(ret), K(json_path));
      } else {
        common::ObString key(seg_end - seg_start, json_path.ptr() + seg_start);
        const bool is_leaf = (seg_end >= json_path.length());
        common::ObIJsonBase *child = cur_obj->get_value(key);
        if (is_leaf) {
          if (OB_NOT_NULL(child)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("duplicated json path in logical table value", K(ret), K(json_path));
          } else if (OB_FAIL(build_json_value_node_(meta, datum, allocator, value_node))) {
            LOG_WARN("failed to build json path value node", K(ret), K(json_path), K(meta));
          } else if (OB_ISNULL(value_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("json path value node is null", K(ret), K(json_path));
          } else if (OB_FAIL(cur_obj->add(key, value_node))) {
            LOG_WARN("failed to append json path leaf", K(ret), K(json_path), K(key));
          }
        } else if (OB_ISNULL(child)) {
          common::ObJsonObject *next_obj = OB_NEWx(common::ObJsonObject, &allocator, &allocator);
          if (OB_ISNULL(next_obj)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate json object for path", K(ret), K(json_path));
          } else if (OB_FAIL(cur_obj->add(key, next_obj))) {
            LOG_WARN("failed to append json path object", K(ret), K(json_path), K(key));
          } else {
            cur_obj = next_obj;
          }
        } else if (common::ObJsonNodeType::J_OBJECT != child->json_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("json path conflicts with non-object node", K(ret), K(json_path), K(key));
        } else {
          cur_obj = static_cast<common::ObJsonObject *>(child);
        }
        seg_start = seg_end + 1;
      }
    }
  }
  return ret;
}

void ObLogicalTableEncoder::try_consume_data_(const uint64_t namespace_id, const int64_t bytes)
{
  if (OB_INVALID_NAMESPACE_ID != namespace_id && bytes > 0) {
    share::ObNamespaceRUManager *ns_mgr = MTL(share::ObNamespaceRUManager *);
    if (OB_NOT_NULL(ns_mgr)) {
      // 仅计量，忽略返回值；不得影响编码结果
      (void)ns_mgr->consume_data(namespace_id, bytes);
    }
  }
}

int ObLogicalTableEncoder::build_json_value_node_(
  const common::ObObjMeta &meta,
  const common::ObDatum &datum,
  common::ObIAllocator &allocator,
  common::ObJsonNode *&value_node)
{
  int ret = OB_SUCCESS;
  value_node = nullptr;
  const common::ObObjType type = meta.get_type();
  common::ObString str;
  common::ObString json_binary;
  common::ObIJsonBase *json_base = nullptr;
  sql::ObDatumMeta datum_meta;
  datum_meta.type_ = meta.get_type();
  datum_meta.cs_type_ = meta.get_collation_type();
  datum_meta.scale_ = meta.get_scale();

  if (datum.is_null()) {
    if (OB_ISNULL(value_node = OB_NEWx(common::ObJsonNull, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate json null node", K(ret));
    }
  } else {
    switch (type) {
      case ObTinyIntType:
      case ObIntType:
      case ObTimestampType:
        if (OB_ISNULL(value_node = OB_NEWx(common::ObJsonInt, &allocator, datum.get_int()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate json int node", K(ret), K(type));
        }
        break;
      case ObUInt64Type:
        if (OB_ISNULL(value_node = OB_NEWx(common::ObJsonUint, &allocator, datum.get_uint()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate json uint node", K(ret), K(type));
        }
        break;
      case ObFloatType:
        if (OB_ISNULL(value_node = OB_NEWx(common::ObJsonDouble, &allocator, datum.get_float()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate json float node", K(ret), K(type));
        }
        break;
      case ObDoubleType:
        if (OB_ISNULL(value_node = OB_NEWx(common::ObJsonDouble, &allocator, datum.get_double()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate json double node", K(ret), K(type));
        }
        break;
      case ObVarcharType:
      case ObCharType:
      case ObTextType:
      case ObLongTextType:
      {
        common::ObCharsetType charset = common::ObCharset::charset_type_by_coll(meta.get_collation_type());
        if (OB_UNLIKELY(common::CHARSET_UTF8MB4 != charset && common::CHARSET_BINARY != charset)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only utf8mb4 or binary strings are supported", K(ret), K(meta));
        } else {
          str = datum.get_string();
          if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(
              allocator, datum, datum_meta, str))) {
            LOG_WARN("failed to extract string datum", K(ret), K(meta));
          } else if (OB_ISNULL(value_node = OB_NEWx(common::ObJsonString, &allocator, str))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate json string node", K(ret), K(str.length()));
          }
        }
        break;
      }
      case ObJsonType:
        json_binary = datum.get_string();
        if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(
            allocator, datum, datum_meta, json_binary))) {
          LOG_WARN("failed to extract json datum", K(ret), K(meta));
        } else if (OB_FAIL(common::ObJsonBaseFactory::get_json_base(
            &allocator,
            json_binary,
            common::ObJsonInType::JSON_BIN,
            common::ObJsonInType::JSON_TREE,
            json_base))) {
          LOG_WARN("failed to parse json datum", K(ret), K(json_binary.length()));
        } else if (OB_ISNULL(value_node = static_cast<common::ObJsonNode *>(json_base))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("json value node is null", K(ret));
        }
        break;
      case ObCollectionSQLType:
        // Vector columns: encode raw float* (or T*) binary as JsonString payload.
        str = datum.get_string();
        if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(
            allocator, datum, datum_meta, str))) {
          LOG_WARN("failed to extract vector datum", K(ret), K(meta));
        } else if (OB_ISNULL(value_node = OB_NEWx(common::ObJsonString, &allocator, str))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate json string node for vector", K(ret), K(str.length()));
        }
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported value column type", K(ret), K(type), K(meta));
        break;
    }
  }
  return ret;
}


int ObLogicalTableEncoder::calc_encoded_key_len_(
  const ObLogicalTableAuxType aux_type,
  const common::ObIArray<common::ObDatum> &datums,
  int64_t &encoded_len)
{
  int ret = OB_SUCCESS;
  const ObLogicalTableKeyColDesc *col_descs = nullptr;
  int64_t col_cnt = 0;
  encoded_len = 0;

  if (OB_FAIL(get_aux_type_col_descs(aux_type, col_descs, col_cnt))) {
    LOG_WARN("failed to get aux type column descriptors", K(ret), K(aux_type));
  } else if (OB_UNLIKELY(datums.count() > col_cnt || OB_ISNULL(col_descs))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key datum layout", K(ret), K(aux_type), K(datums.count()), K(col_cnt), KP(col_descs));
  } else {
    encoded_len = OB_LOGICAL_TABLE_KEY_PREFIX_LEN;
    for (int64_t i = 0; OB_SUCC(ret) && i < datums.count(); ++i) {
      const ObLogicalTableKeyColDesc &col_desc = col_descs[i];
      const common::ObDatum &datum = datums.at(i);
      if (OB_FAIL(validate_key_datum_(col_desc, datum))) {
        LOG_WARN("invalid logical key datum", K(ret), K(i), K(col_desc),
                 K(datum.is_null()),
                 "datum_length", datum.is_null() ? 0 : datum.get_string().length());
      } else {
        if (col_desc.is_nullable_) {
          encoded_len += 1;
        }
        if (datum.is_null()) {
        } else {
          switch (col_desc.type_) {
            case ObUTinyIntType:
              encoded_len += sizeof(uint8_t);
              break;
            case ObUInt64Type:
              encoded_len += sizeof(uint64_t);
              break;
            case ObIntType:
              encoded_len += sizeof(int64_t);
              break;
            case ObVarcharType:
              if (col_desc.is_order_preserving_encoded_) {
                encoded_len += static_cast<int64_t>(datum.get_string().length());
              } else {
                encoded_len += static_cast<int64_t>(datum.get_string().length()) * 2 + 2;
              }
              break;
            default:
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("unsupported key column type", K(ret), K(i), K(col_desc));
              break;
          }
        }
      }
    }
  }

  return ret;
}

int ObLogicalTableEncoder::encode_key_to_buf_(
  const ObLogicalTableKeyPrefix &prefix,
  const common::ObIArray<common::ObDatum> &datums,
  char *buf,
  const int64_t buf_len,
  int64_t &pos)
{
  int ret = OB_SUCCESS;
  const ObLogicalTableKeyColDesc *col_descs = nullptr;
  int64_t col_cnt = 0;

  if (OB_UNLIKELY(!prefix.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for key encoding", K(ret), K(prefix));
  } else if (OB_FAIL(get_aux_type_col_descs(prefix.aux_type_, col_descs, col_cnt))) {
    LOG_WARN("failed to get key column descriptors", K(ret), K(prefix.aux_type_));
  } else if (OB_UNLIKELY(datums.count() > col_cnt || OB_ISNULL(col_descs))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key datum count", K(ret), K(datums.count()), K(col_cnt), KP(col_descs));
  } else if (OB_FAIL(prefix.encode(buf, buf_len, pos))) {
    LOG_WARN("failed to encode key prefix", K(ret), K(prefix), K(buf_len), K(pos));
  } else {
    unsigned char *dst = reinterpret_cast<unsigned char *>(buf);
    for (int64_t i = 0; OB_SUCC(ret) && i < datums.count(); ++i) {
      if (OB_FAIL(encode_key_column(col_descs[i], datums.at(i), dst, buf_len, pos))) {
        LOG_WARN("failed to encode key column", K(ret), K(i), K(col_descs[i]));
      }
    }
  }

  return ret;
}

int ObLogicalTableEncoder::encode_key_column(
  const ObLogicalTableKeyColDesc &col_desc,
  const common::ObDatum &datum,
  unsigned char *buf,
  const int64_t buf_len,
  int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key encode buffer is null", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(validate_key_datum_(col_desc, datum))) {
    LOG_WARN("invalid logical key datum", K(ret), K(col_desc), K(datum.is_null()),
             "datum_length", datum.is_null() ? 0 : datum.get_string().length());
  } else if (col_desc.is_nullable_ && OB_FAIL(encode_nullable_flag_(datum.is_null(), buf, buf_len, pos))) {
    LOG_WARN("failed to encode nullable flag", K(ret), K(col_desc));
  } else if (datum.is_null()) {
  } else {
    switch (col_desc.type_) {
      case ObUTinyIntType:
        ret = ObLogicalTableCodecUtils::encode_uint8(datum.get_utinyint(), buf, buf_len, pos);
        break;
      case ObUInt64Type:
        ret = ObLogicalTableCodecUtils::encode_uint64(datum.get_uint(), buf, buf_len, pos);
        break;
      case ObIntType:
        ret = ObLogicalTableCodecUtils::encode_int64(datum.get_int(), buf, buf_len, pos);
        break;
      case ObVarcharType:
        if (col_desc.is_order_preserving_encoded_) {
          ret = ObLogicalTableCodecUtils::append_order_preserving_binary(
            datum.get_string().ptr(), datum.get_string().length(), buf, buf_len, pos);
        } else {
          ret = ObLogicalTableCodecUtils::encode_varchar_bin(
            datum.get_string().ptr(), datum.get_string().length(), buf, buf_len, pos);
        }
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported key column type", K(ret), K(col_desc));
        break;
    }
  }
  return ret;
}

int ObLogicalTableEncoder::validate_key_datum_(
  const ObLogicalTableKeyColDesc &col_desc,
  const common::ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!col_desc.is_nullable_ && datum.is_null())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!datum.is_null()
      && col_desc.fixed_length_ > 0
      && datum.get_string().length() != col_desc.fixed_length_)) {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObLogicalTableEncoder::encode_nullable_flag_(
  const bool is_null,
  unsigned char *buf,
  const int64_t buf_len,
  int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf) || pos < 0 || pos >= buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough for nullable flag", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    buf[pos++] = is_null ? 0x00 : 0x01;
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
