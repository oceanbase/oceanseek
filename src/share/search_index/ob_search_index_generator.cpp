/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/search_index/ob_search_index_generator.h"
#include "lib/charset/ob_charset.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/hash/ob_hashset.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/number/ob_number_v2.h"
#include "share/vector/ob_fixed_length_base.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/udt/ob_array_utils.h"

namespace oceanbase
{
namespace share
{


int ObSearchIndexRow::build_datum_row(ObIAllocator &allocator, blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = has_nested_key_ ? SEARCH_INDEX_KEY_CNT_WITH_NK : SEARCH_INDEX_KEY_CNT;
  if (OB_FAIL(datum_row.init(allocator, col_cnt))) {
    LOG_WARN("fail to init datum row", K(ret));
  } else {
    datum_row.storage_datums_[SEARCH_INDEX_COL_IDX].set_uint(column_idx_);
    datum_row.storage_datums_[SEARCH_INDEX_PATH].shallow_copy_from_datum(path_);
    datum_row.storage_datums_[SEARCH_INDEX_VALUE].shallow_copy_from_datum(value_);
    datum_row.storage_datums_[SEARCH_INDEX_DOC_ID].set_uint(doc_id_);
    if (has_nested_key_) {
      datum_row.storage_datums_[SEARCH_INDEX_NESTED_KEY].shallow_copy_from_datum(nested_key_);
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::Generator::init(const uint64_t column_idx, const ObObjMeta &obj_meta,
                                               const common::ObCollectionArrayType *arr_type,
                                               const ObSearchIndexConfigFilter *config_filter)
{
  int ret = OB_SUCCESS;
  column_idx_ = column_idx;
  obj_meta_ = obj_meta;
  const ObObjType col_type = obj_meta_.get_type();
  if (ob_is_json(col_type)) {
    if (OB_FAIL(ObSearchIndexValueEncoder::init_json_string_enc_param(enc_param_))) {
      LOG_WARN("fail to init json string enc param", K(ret));
    }
  } else if (ob_is_string_type(col_type)) {
    ObObjType obj_type = ob_is_text_tc(col_type) ? ObVarcharType : obj_meta_.get_type();
    ObCollationType cs_type = obj_meta_.get_collation_type();
    ObCharsetType charset = ObCharset::charset_type_by_coll(cs_type);
    if (CHARSET_UTF16 == charset || CHARSET_UTF16LE == charset) {
      orig_cs_type_ = cs_type;
      need_normalize_encoding_ = true;
      cs_type = ObCharset::is_bin_sort(cs_type) ? CS_TYPE_UTF8MB4_BIN
                                                : CS_TYPE_UTF8MB4_GENERAL_CI;
    }
    if (OB_FAIL(ObSearchIndexValueEncoder::init_string_enc_param(obj_type,
                                                                 cs_type,
                                                                 0, /** sql_mode */
                                                                 true, /** is_null_first */
                                                                 true, /** is_asc */
                                                                 enc_param_))) {
      LOG_WARN("fail to init string enc param", K(ret));
    }
  } else if (ObCollectionSQLType == col_type) {
    if (OB_FAIL(init_array_generator(arr_type))) {
      LOG_WARN("fail to init array generator", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    config_filter_ = config_filter;
    has_nested_paths_ = config_filter_ && config_filter_->has_nested_paths();
  }
  return ret;
}

int ObSearchIndexRowGenerator::Generator::init_array_generator(const common::ObCollectionArrayType *arr_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arr_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arr_type is null", K(ret));
  } else {
    // deep copy arr_type to inner_allocator_ so its lifetime is managed by generator
    common::ObCollectionTypeBase *dst = nullptr;
    if (OB_FAIL(arr_type->deep_copy(inner_allocator_, dst))) {
      LOG_WARN("fail to deep copy arr type", K(ret));
    } else {
      arr_type_ = static_cast<common::ObCollectionArrayType *>(dst);
      uint32_t depth = 0;
      const ObDataType &basic_elem = arr_type_->get_basic_meta(depth);
      char path_buf[64];
      if (OB_FAIL(common::ObArrayUtil::get_type_name(static_cast<common::ObNestedType>(arr_type_->type_id_),
                                                     basic_elem, path_buf, sizeof(path_buf)))) {
        LOG_WARN("fail to get array type name", K(ret));
      } else {
        const int32_t path_len = static_cast<int32_t>(strlen(path_buf));
        char *copy = static_cast<char *>(inner_allocator_.alloc(path_len));
        if (OB_ISNULL(copy)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc array index path", K(ret));
        } else {
          MEMCPY(copy, path_buf, path_len);
          array_index_path_.assign_ptr(copy, path_len);
          ObObjMeta elem_meta = basic_elem.get_meta_type();
          if (elem_meta.is_varchar() || elem_meta.is_text()) {
            // ObArrayBinary compares bytes, including case, trailing spaces and NULs.
            // Use the same encoding for index entries and array query values.
            elem_meta.set_collation_type(CS_TYPE_BINARY);
          }
          void *elem_generator_ptr = nullptr;
          if (OB_ISNULL(elem_generator_ptr = inner_allocator_.alloc(sizeof(Generator)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory for element generator", K(ret));
          } else {
            elem_generator_ = new (elem_generator_ptr) Generator(allocator_, inner_allocator_, has_nested_key_);
            if (OB_FAIL(elem_generator_->init(column_idx_, elem_meta, nullptr, nullptr /* config_filter */))) {
              LOG_WARN("fail to init element generator", K(ret));
            } else {
              elem_generator_->array_index_path_ = array_index_path_;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::Generator::push_back_row(ObSearchIndexRows &rows, ObSearchIndexRow &row)
{
  if (has_nested_key_) {
    row.has_nested_key_ = true;
  }
  return rows.push_back(row);
}

int ObSearchIndexRowGenerator::Generator::generate_rows(
    const int64_t doc_id, const ObDatum& datum,
    ObSearchIndexRows &rows)
{
  int ret = OB_SUCCESS;
  const ObObjType obj_type = obj_meta_.get_type();
  if (OB_UNLIKELY(datum.is_null())) {
    // datum null: column_idx, null, null, doc_id[, null nested_key]
    ObSearchIndexRow row(column_idx_, doc_id);
    if (OB_FAIL(push_back_row(rows, row))) {
      LOG_WARN("fail to push back gin row", K(ret));
    }
  } else if (ob_is_json(obj_type)) {
    if (OB_FAIL(generate_json_rows(doc_id, datum, rows))) {
      LOG_WARN("fail to generate json gin rows", K(ret));
    }
  } else if (ob_is_collection_sql_type(obj_type)) {
    if (OB_FAIL(generate_array_rows(doc_id, datum, rows))) {
      LOG_WARN("fail to generate array gin rows", K(ret));
    }
  } else {
    // scalar type, one datum generate one row
    ObString value;
    switch (obj_type) {
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType: {
        ret = ObSearchIndexValueEncoder::encode_int(allocator_, datum.get_int(), value);
        break;
      }
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type: {
        ret = ObSearchIndexValueEncoder::encode_uint(allocator_, datum.get_uint(), value);
        break;
      }
      case ObFloatType:
      case ObUFloatType: {
        ret = ObSearchIndexValueEncoder::encode_float(allocator_, datum.get_float(), value);
        break;
      }
      case ObDoubleType:
      case ObUDoubleType: {
        ret = ObSearchIndexValueEncoder::encode_double(allocator_, datum.get_double(), value);
        break;
      }
      case ObVarcharType:
      case ObCharType: {
        ObString str_value = datum.get_string();
        ObCollationType tmp_cs = orig_cs_type_;
        if (need_normalize_encoding_
            && OB_FAIL(ObSearchIndexValueEncoder::normalize_string_for_encoding(
                   allocator_, str_value, tmp_cs))) {
          LOG_WARN("fail to normalize string for encoding", K(ret));
        } else if (OB_FAIL(ObSearchIndexValueEncoder::encode_string(
                       allocator_, str_value, value, enc_param_))) {
          LOG_WARN("fail to encode string", K(ret));
        }
        break;
      }
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType: {
        ObString str_value = datum.get_string();
        ObCollationType tmp_cs = orig_cs_type_;
        if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(
                &allocator_, obj_meta_.get_type(), obj_meta_.get_collation_type(), str_value))) {
          LOG_WARN("fail to read real string data", K(ret));
        } else if (need_normalize_encoding_
                   && OB_FAIL(ObSearchIndexValueEncoder::normalize_string_for_encoding(
                          allocator_, str_value, tmp_cs))) {
          LOG_WARN("fail to normalize string for encoding", K(ret));
        } else if (OB_FAIL(ObSearchIndexValueEncoder::encode_string(
                       allocator_, str_value, value, enc_param_))) {
          LOG_WARN("fail to encode string", K(ret));
        }
        break;
      }
      case ObDateType:
      case ObMySQLDateType: {
        ret = ObSearchIndexValueEncoder::encode_int32(allocator_, datum.get_date(), value);
        break;
      }
      case ObDateTimeType:
      case ObTimestampType:
      case ObMySQLDateTimeType: {
        ret = ObSearchIndexValueEncoder::encode_int(allocator_, datum.get_datetime(), value);
        break;
      }
      case ObTimeType: {
        ret = ObSearchIndexValueEncoder::encode_int(allocator_, datum.get_time(), value);
        break;
      }
      case ObYearType: {
        ret = ObSearchIndexValueEncoder::encode_uint8(allocator_, datum.get_year(), value);
        break;
      }
      case ObNumberType:
      case ObUNumberType: {
        const number::ObNumber number(datum.get_number());
        ret = ObSearchIndexValueEncoder::encode_number(allocator_, number, value);
        break;
      }
      case ObDecimalIntType: {
        ret = ObSearchIndexValueEncoder::encode_decimal_int(allocator_, datum.get_decimal_int(),
                                                            datum.get_int_bytes(), value);
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported object type", K(ret), K(obj_type));
      }
    }
    if (OB_SUCC(ret)) {
      if (!array_index_path_.empty()) {
        ObSearchIndexRow row(column_idx_, array_index_path_, value, doc_id);
        if (OB_FAIL(push_back_row(rows, row))) {
          LOG_WARN("fail to push back gin row", K(ret));
        }
      } else {
        ObSearchIndexRow row(column_idx_, value, doc_id);
        if (OB_FAIL(push_back_row(rows, row))) {
          LOG_WARN("fail to push back gin row", K(ret));
        }
      }
    }
  }
  return ret;
}


int ObSearchIndexRowGenerator::Generator::generate_rows(const ObIVector &doc_id_vec,
                                                        const ObIVector &data_vec,
                                                        const int64_t batch_size,
                                                        ObSearchIndexRows &rows)
{
  return OB_NOT_IMPLEMENT;
}

int ObSearchIndexRowGenerator::Generator::generate_json_rows(const int64_t doc_id,
                                                             const ObDatum& datum,
                                                             ObSearchIndexRows &rows)
{
  int ret = OB_SUCCESS;
  ObString j_bin_str = datum.get_string();
  ObIJsonBase *j_base = nullptr;
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 16> path_items;
  if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(
      &allocator_, ObJsonType, CS_TYPE_UTF8MB4_BIN, j_bin_str))) {
    LOG_WARN("fail to read real string data", K(ret));
  } else if (OB_FAIL(common::ObJsonBaseFactory::get_json_base(
                                                              &allocator_,
                                                              j_bin_str,
                                                              common::ObJsonInType::JSON_BIN,
                                                              common::ObJsonInType::JSON_BIN,
                                                              j_base))) {
    LOG_WARN("fail to get json base", K(ret));
  } else {
    // document-level (path, value) dedup to avoid duplicate key for nested array same values
    PathValueDedupSet path_value_dedup_set;
    if (OB_FAIL(path_value_dedup_set.create(256, ObMemAttr(MTL_ID(), "SeaIdxValDeDup")))) {
      LOG_WARN("fail to create path value dedup set", K(ret));
    } else if (OB_FAIL(generate_rows(doc_id, j_base, path_items, rows, path_value_dedup_set))) {
      LOG_WARN("failed to generate gin rows", K(ret));
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::Generator::generate_rows(
    const int64_t doc_id, ObIJsonBase *j_base,
    ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
    ObSearchIndexRows &rows,
    PathValueDedupSet &path_value_dedup_set)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(j_base)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json base is null", K(ret));
  } else {
    ObJsonNodeType node_type = j_base->json_type();
    const int64_t elem_cnt = j_base->element_count();
    const bool is_empty = (elem_cnt == 0);
    if (ObJsonNodeType::J_OBJECT == node_type && !is_empty) {
      JsonObjectIterator iter(j_base);
      while (OB_SUCC(ret) && !iter.end()) {
        common::ObJsonObjPair elem;
        if (OB_FAIL(iter.get_elem(elem))) {
          LOG_WARN("fail to get elem", K(ret));
        } else if (OB_FAIL(path_items.push_back(ObSearchIndexPathEncoder::make_object_path(elem.first)))) {
          LOG_WARN("fail to push path string", K(ret));
        } else if (OB_FAIL(generate_rows(doc_id, elem.second, path_items, rows, path_value_dedup_set))) {
          LOG_WARN("fail to generate gin rows", K(ret));
        } else {
          path_items.pop_back();
          iter.next();
        }
      }
    } else if (ObJsonNodeType::J_ARRAY == node_type && !is_empty) {
      // Check if this array is a declared NESTED_PATH entry; if so, track array indices
      // in nested_key_stack_ so scalar descendants can encode their per-element identity.
      bool is_nested = false;
      if (has_nested_paths_ && OB_NOT_NULL(config_filter_)
          && OB_FAIL(config_filter_->is_nested_path(path_items, is_nested))) {
        LOG_WARN("failed to check nested path", K(ret));
      } else if (OB_FAIL(path_items.push_back(ObSearchIndexPathEncoder::make_array_path()))) {
        LOG_WARN("fail to push path string", K(ret));
      } else {
        for (uint64_t i = 0; OB_SUCC(ret) && i < elem_cnt; ++i) {
          ObIJsonBase *elem = nullptr;
          if (is_nested && OB_FAIL(nested_key_stack_.push_back(static_cast<uint32_t>(i)))) {
            LOG_WARN("failed to push nested key stack", K(ret), K(i));
          } else if (is_nested && OB_UNLIKELY(nested_key_stack_.count() > ObSearchIndexNestedKeyEncoder::MAX_DEPTH)) {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("nested key depth exceeds MAX_DEPTH", K(ret), K(nested_key_stack_.count()),
              K(ObSearchIndexNestedKeyEncoder::MAX_DEPTH));
          } else if (OB_FAIL(j_base->get_array_element(i, elem))) {
            LOG_WARN("fail to get array elem", K(ret), K(i), K(elem_cnt));
          } else if (OB_FAIL(generate_rows(doc_id, elem, path_items, rows, path_value_dedup_set))) {
            LOG_WARN("fail to generate gin rows for array elem", K(ret));
          }
          if (OB_SUCC(ret) && is_nested && !nested_key_stack_.empty()) {
            nested_key_stack_.pop_back();
          }
        }
        path_items.pop_back();
      }
    } else {
      // scalar json node
      ObString path;
      ObString value;
      const bool is_json_scalar = j_base->is_json_scalar(node_type);
      bool passed = true;
      if (OB_NOT_NULL(config_filter_) && OB_FAIL(config_filter_->is_indexed(path_items, node_type, passed))) {
        LOG_WARN("failed to check path filter", K(ret));
      } else if (!passed) {
        // do nothing
      } else if (is_json_scalar && OB_FAIL(ObSearchIndexValueEncoder::encode_json_scalar(allocator_,
                                                                                         j_base,
                                                                                         value,
                                                                                         enc_param_))) {
        LOG_WARN("fail to encode json scalar", K(ret));
      } else if (OB_FAIL(ObSearchIndexPathEncoder::encode_path(allocator_, path_items, node_type,
                                                               path))) {
        LOG_WARN("fail to encode path", K(ret));
      } else {
        // Encode nested_key for this scalar (empty string for non-nested paths).
        ObString nested_key_str;
        if (has_nested_paths_ && !nested_key_stack_.empty()
            && OB_FAIL(ObSearchIndexNestedKeyEncoder::encode(allocator_, nested_key_stack_,
                                                             nested_key_str))) {
          LOG_WARN("failed to encode nested key", K(ret));
        } else {
          // document-level dedup: skip if same (path, value, nested_key) already emitted
          PathValueKey key;
          key.path_ = path;
          key.value_ = value;
          key.nested_key_ = nested_key_str;
          int tmp_ret = path_value_dedup_set.set_refactored(key, 0);
          if (tmp_ret == OB_HASH_EXIST) {
            // already emitted this (path, value, nested_key), skip to avoid duplicate key
          } else if (tmp_ret != OB_SUCCESS) {
            ret = tmp_ret;
            LOG_WARN("fail to insert to path value dedup set", K(ret));
          } else {
            ObSearchIndexRow row(column_idx_, path, value, doc_id);
            if (!nested_key_stack_.empty()) {
              row.nested_key_.set_string(nested_key_str);
            }
            if (OB_FAIL(push_back_row(rows, row))) {
              LOG_WARN("fail to push back gin row", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::Generator::generate_rows_for_array_obj(
    const int64_t doc_id,
    common::ObIArrayType *arr_obj,
    ObSearchIndexRows &rows)
{
  int ret = OB_SUCCESS;
  common::ObIArrayType *distinct_arr_obj = nullptr;
  if (OB_ISNULL(arr_obj) || OB_ISNULL(elem_generator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(arr_obj), KP_(elem_generator));
  } else if (arr_obj->get_format() == ArrayFormat::Nested_Array) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("nested array is not supported for now", K(ret));
  } else if (OB_FAIL(arr_obj->distinct(allocator_, distinct_arr_obj))) {
    LOG_WARN("fail to distinct array", K(ret));
  } else if (OB_FAIL(distinct_arr_obj->init())) {
    LOG_WARN("array init failed", K(ret));
  } else {
    uint32_t len = distinct_arr_obj->size();
    for (uint32_t i = 0; OB_SUCC(ret) && i < len; ++i) {
      if (distinct_arr_obj->is_null(i)) {
        ObSearchIndexRow row(column_idx_, doc_id, array_index_path_);
        if (OB_FAIL(push_back_row(rows, row))) {
          LOG_WARN("fail to push back gin row", K(ret));
        }
      } else {
        common::ObObj elem_obj;
        ObDatum elem_datum;
        char elem_datum_buf[common::OBJ_DATUM_MAX_RES_SIZE] = {0};
        elem_datum.ptr_ = elem_datum_buf;
        if (OB_FAIL(distinct_arr_obj->elem_at(i, elem_obj))) {
          LOG_WARN("fail to get array element", K(ret), K(i));
        } else if (OB_FAIL(elem_datum.from_obj(elem_obj))) {
          LOG_WARN("fail to convert obj to datum", K(ret), K(elem_obj));
        } else if (OB_FAIL(elem_generator_->generate_rows(doc_id, elem_datum, rows))) {
          LOG_WARN("fail to generate gin rows for array element", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::Generator::generate_array_rows(const int64_t doc_id,
                                                            const ObDatum& datum,
                                                            ObSearchIndexRows &rows)
{
  int ret = OB_SUCCESS;
  ObString data_str = datum.get_string();
  if (OB_ISNULL(arr_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arr_type is null", K(ret));
  } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator_,
                                      ObLongTextType,
                                      CS_TYPE_BINARY,
                                      data_str))) {
    LOG_WARN("fail to get real data.", K(ret), K(data_str));
  } else {
    uint32_t len = 0, data_len = 0;
    uint8_t *null_bitmaps = nullptr;
    const char *data = nullptr;
    common::ObIArrayType *arr_obj = nullptr;
    if (OB_FAIL(sql::ObArrayExprUtils::get_array_data(data_str,
                                        arr_type_,
                                        len,
                                        null_bitmaps,
                                        data,
                                        data_len))) {
      LOG_WARN("failed to get array data", K(ret));
    } else if (OB_FAIL(common::ObArrayTypeObjFactory::construct(allocator_, *arr_type_, arr_obj, true))) {
      LOG_WARN("fail to construct array obj", K(ret));
    } else if (OB_FAIL(arr_obj->init(data_str))) {
      LOG_WARN("fail to init array obj", K(ret));
    } else if (OB_FAIL(generate_rows_for_array_obj(doc_id, arr_obj, rows))) {
      LOG_WARN("fail to generate gin rows from array", K(ret));
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::init(const common::ObIArray<int32_t> &included_cid_idxes,
                                    const common::ObIArray<ObObjMeta> &included_obj_metas,
                                    const common::ObIArray<int64_t> &row_projector,
                                    const common::ObIArray<ObString> &column_comments,
                                    bool rowkey_only,
                                    const common::ObIArray<common::ObCollectionArrayType*> *arr_types)
{
  int ret = OB_SUCCESS;
  UNUSED(rowkey_only);
  CK (included_cid_idxes.count() == included_obj_metas.count());
  CK (included_cid_idxes.count() == column_comments.count());
  void *mem = nullptr;
  bool has_nested_key = false;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(config_filters_.prepare_allocate(column_comments.count()))) {
    LOG_WARN("failed to prepare allocate search index config filters", K(ret), K(column_comments.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_comments.count(); ++i) {
      const ObString &comment = column_comments.at(i);
      if (!comment.empty()) {
        ObSearchIndexConfigFilter *filter = OB_NEWx(ObSearchIndexConfigFilter, &inner_allocator_, MTL_ID());
        if (OB_ISNULL(filter)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc search index json path filter", K(ret));
        } else if (OB_FAIL(filter->init_from_comment(comment))) {
          LOG_WARN("failed to init config filter from column comment", K(ret), K(comment));
          filter->~ObSearchIndexConfigFilter();
        } else {
          config_filters_[i] = filter;
          if (filter->has_nested_paths()) {
            has_nested_key = true;
          }
        }
      } else {
        config_filters_[i] = nullptr;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_projector_.assign(row_projector))) {
    LOG_WARN("failed to assign row projector", K(ret));
  } else if (OB_FAIL(generators_.prepare_allocate(included_cid_idxes.count()))) {
    LOG_WARN("fail to init generators", K(ret));
  } else if (OB_ISNULL(mem = inner_allocator_.alloc(sizeof(Generator) * included_cid_idxes.count()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else if (OB_NOT_NULL(arr_types) && included_cid_idxes.count() != arr_types->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("included cid idxes count mismatch with arr types count", K(ret),
             K(included_cid_idxes.count()), K(arr_types->count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < included_cid_idxes.count(); ++i) {
      generators_.at(i) = new (static_cast<char*>(mem) + i * sizeof(Generator)) Generator(row_allocator_, inner_allocator_, has_nested_key);
      const common::ObCollectionArrayType *arr_type = (nullptr != arr_types) ? arr_types->at(i) : nullptr;
      if (OB_FAIL(generators_.at(i)->init(included_cid_idxes.at(i), included_obj_metas.at(i),
                                          arr_type, config_filters_.at(i)))) {
        LOG_WARN("fail to init generator", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      row_projector_ptr_ = &row_projector_;
      has_nested_key_ = has_nested_key;
    }
  }
  return ret;
}

void ObSearchIndexRowGenerator::reset()
{
  for (int64_t i = 0; i < config_filters_.count(); ++i) {
    ObSearchIndexConfigFilter *filter = config_filters_.at(i);
    if (OB_NOT_NULL(filter)) {
      filter->~ObSearchIndexConfigFilter();
    }
  }
  generators_.reset();
  row_projector_.reset();
  config_filters_.reset();
  inner_allocator_.reset();
  row_allocator_.reset();
  row_projector_ptr_ = nullptr;
  has_nested_key_ = false;
}

int ObSearchIndexRowGenerator::set_row_projector(const common::ObFixedArray<int64_t, ObIAllocator> *row_projector)
{
  int ret = OB_SUCCESS;
  row_projector_ptr_ = row_projector;
  return ret;
}

int ObSearchIndexRowGenerator::generate_rows(const sql::ObChunkDatumStore::StoredRow *store_row,
                                             blocksstable::ObDatumRow *&rows, int64_t& row_cnt)
{
  int ret = OB_SUCCESS;
  rows = nullptr;
  row_cnt = 0;
  if (OB_ISNULL(store_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("store row is null", K(ret));
  } else if (OB_ISNULL(row_projector_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row projector is null", K(ret));
  } else {
    // reuse row allocator for each generate_rows call
    row_allocator_.reuse();
    ObSearchIndexRows index_rows;
    const int64_t doc_proj = row_projector_ptr_->at(generators_.count());
    const uint64_t doc_id = store_row->cells()[doc_proj].get_uint();
    for (int64_t i = 0; OB_SUCC(ret) && i < generators_.count(); ++i) {
      const int64_t project_idx = row_projector_ptr_->at(i);
      if (OB_FAIL(generators_.at(i)->generate_rows(doc_id, store_row->cells()[project_idx], index_rows))) {
        LOG_WARN("fail to generate gin rows", K(ret));
      }
    }
    if (OB_SUCC(ret) && index_rows.count() > 0) {
      int64_t index_rows_cnt = index_rows.count();
      void *rows_buf = row_allocator_.alloc(sizeof(blocksstable::ObDatumRow) * index_rows_cnt);
      if (OB_ISNULL(rows_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else {
        rows = new (rows_buf) blocksstable::ObDatumRow[index_rows_cnt];
        for (int64_t i = 0; OB_SUCC(ret) && i < index_rows.count(); ++i) {
          if (OB_FAIL(index_rows.at(i).build_datum_row(row_allocator_, rows[i]))) {
            LOG_WARN("fail to build datum row", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        row_cnt = index_rows_cnt;
      }
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::generate_rows(const blocksstable::ObDatumRow &store_row,
                                             blocksstable::ObDatumRow *&rows, int64_t& row_cnt)
{
  int ret = OB_SUCCESS;
  rows = nullptr;
  row_cnt = 0;
  if (OB_ISNULL(row_projector_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row projector is null", K(ret));
  } else {
    // reuse row allocator for each generate_rows call
    row_allocator_.reuse();
    ObSearchIndexRows index_rows;
    const int64_t doc_proj = row_projector_ptr_->at(generators_.count());
    const uint64_t doc_id = store_row.storage_datums_[doc_proj].get_uint();
    for (int64_t i = 0; OB_SUCC(ret) && i < generators_.count(); ++i) {
      const int64_t project_idx = row_projector_ptr_->at(i);
      if (OB_FAIL(generators_.at(i)->generate_rows(doc_id, store_row.storage_datums_[project_idx], index_rows))) {
        LOG_WARN("fail to generate gin rows", K(ret));
      }
    }
    if (OB_SUCC(ret) && index_rows.count() > 0) {
      int64_t index_rows_cnt = index_rows.count();
      void *rows_buf = row_allocator_.alloc(sizeof(blocksstable::ObDatumRow) * index_rows_cnt);
      if (OB_ISNULL(rows_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else {
        rows = new (rows_buf) blocksstable::ObDatumRow[index_rows_cnt];
        for (int64_t i = 0; OB_SUCC(ret) && i < index_rows.count(); ++i) {
          if (OB_FAIL(index_rows.at(i).build_datum_row(row_allocator_, rows[i]))) {
            LOG_WARN("fail to build datum row", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        row_cnt = index_rows_cnt;
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
