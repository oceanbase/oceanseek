/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/logical_table/ob_logical_table_decoder.h"

#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/logical_table/ob_logical_table_codec_utils.h"
#include "share/logical_table/ob_logical_table_define.h"

namespace oceanbase
{
namespace share
{

int ObLogicalTableDecoder::decode_key_prefix(
  const char *buf,
  const int64_t buf_len,
  int64_t &pos,
  ObLogicalTableKeyPrefix &prefix)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prefix.decode(buf, buf_len, pos))) {
    LOG_WARN("failed to decode logical table key prefix", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

// ---- internal helpers ----
namespace {

// Compute byte width of a single column starting at `pos` in `src`.
// For varchar: scans varchar_bin terminator (0x00 0x00) when not order-preserving;
// for order-preserving varchar: fixed OB_DOC_ID_COLUMN_BYTE_LENGTH (16B).
// Updates `pos` to point past the column (including the 2B terminator for varchar_bin).
// `width` is the consumed bytes for the column body (excludes the 1B null flag).
// PRECONDITION: for varchar_bin, `src` must be the output of encode_varchar_bin,
// where every raw 0x00 byte is escaped to 0x00 0x01 and the body is terminated by
// 0x00 0x00. Feeding un-escaped data containing raw 0x00 here will mis-detect the
// terminator and walk across the column boundary.
int measure_one_column(
    const unsigned char *src,
    const int64_t buf_len,
    const ObLogicalTableKeyColDesc &desc,
    int64_t &pos,
    int64_t &width)
{
  int ret = OB_SUCCESS;
  width = 0;
  // fixed_width: column body has a fixed byte width and advances `pos` by `pos += width`
  // after a uniform bounds check below. varchar_bin manages `pos` itself (its terminator
  // scan is already bounded by buf_len).
  bool fixed_width = true;
  if (desc.type_ == common::ObUInt64Type) {
    width = static_cast<int64_t>(sizeof(uint64_t));
  } else if (desc.type_ == common::ObIntType) {
    width = static_cast<int64_t>(sizeof(int64_t));
  } else if (desc.type_ == common::ObUTinyIntType) {
    width = 1;
  } else if (desc.type_ == common::ObVarcharType && desc.is_order_preserving_encoded_) {
    width = common::OB_DOC_ID_COLUMN_BYTE_LENGTH; // 16B fixed
  } else if (desc.type_ == common::ObVarcharType) {
    fixed_width = false;
    // Scan varchar_bin terminator (0x00 0x00). Width is body bytes only (excluding terminator).
    const int64_t varchar_start = pos;
    int64_t scan = pos;
    bool found_terminator = false;
    while (scan + 1 < buf_len) {
      if (src[scan] == 0x00 && src[scan + 1] == 0x00) {
        found_terminator = true;
        break;
      } else if (src[scan] == 0x00 && src[scan + 1] == 0x01) {
        scan += 2;
      } else {
        scan += 1;
      }
    }
    if (!found_terminator) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("varchar_bin terminator 0x00 0x00 not found",
               K(ret), K(buf_len), K(varchar_start), K(scan));
    } else {
      width = scan - varchar_start;
      pos = scan + 2; // skip body + 2B terminator
    }
  } else {
    fixed_width = false;
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported column type in key", K(ret), K(desc.type_));
  }
  // Guard fixed-width advancement so a truncated/corrupted key cannot push `pos`
  // past the buffer end (which would yield an out-of-range col_offset/col_width).
  if (OB_SUCC(ret) && fixed_width) {
    if (OB_UNLIKELY(pos < 0 || width > buf_len - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fixed-width key column exceeds buffer",
               K(ret), K(pos), K(width), K(buf_len), K(desc.type_));
    } else {
      pos += width;
    }
  }
  return ret;
}

} // anonymous namespace

int ObLogicalTableDecoder::compute_key_column_offset(
    const common::ObString &key,
    const ObLogicalTableAuxType aux_type,
    const int64_t ordinal,
    int64_t &col_offset,
    int64_t &col_width)
{
  int ret = OB_SUCCESS;
  col_offset = -1;
  col_width = 0;
  if (OB_UNLIKELY(key.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty logical table key", K(ret));
  } else if (OB_UNLIKELY(ordinal < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ordinal", K(ret), K(ordinal));
  } else {
    const ObLogicalTableKeyColDesc *col_descs = nullptr;
    int64_t col_cnt = 0;
    if (OB_FAIL(get_aux_type_col_descs(aux_type, col_descs, col_cnt))) {
      LOG_WARN("failed to get aux type col descs", K(ret), K(aux_type));
    } else if (OB_UNLIKELY(ordinal >= col_cnt)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ordinal out of range", K(ret), K(ordinal), K(col_cnt), K(aux_type));
    } else {
      const unsigned char *src = reinterpret_cast<const unsigned char *>(key.ptr());
      const int64_t buf_len = key.length();
      int64_t pos = OB_LOGICAL_TABLE_KEY_PREFIX_LEN;
      // Walk preceding columns to advance `pos`.
      for (int64_t j = 0; OB_SUCC(ret) && j < ordinal; ++j) {
        const ObLogicalTableKeyColDesc &d = col_descs[j];
        if (d.is_nullable_) {
          if (pos >= buf_len) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("key truncated when reading null flag", K(ret), K(pos), K(buf_len), K(j));
          } else {
            // encoder writes 0x00 for NULL, 0x01 for non-NULL (see encode_nullable_flag_)
            const bool is_null = (src[pos] == 0);
            pos += 1;
            if (is_null) {
              // null column has 0 body bytes
              continue;
            }
          }
        }
        int64_t w = 0;
        if (OB_SUCC(ret) && OB_FAIL(measure_one_column(src, buf_len, d, pos, w))) {
          LOG_WARN("measure preceding column failed", K(ret), K(j), K(d));
        }
      }
      if (OB_SUCC(ret)) {
        const ObLogicalTableKeyColDesc &target = col_descs[ordinal];
        if (target.is_nullable_) {
          if (pos >= buf_len) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("key truncated at target null flag", K(ret), K(pos), K(buf_len));
          } else {
            // encoder writes 0x00 for NULL, 0x01 for non-NULL (see encode_nullable_flag_)
            const bool is_null = (src[pos] == 0);
            pos += 1;
            if (is_null) {
              col_offset = pos;
              col_width = 0;
            } else {
              col_offset = pos;
              if (OB_FAIL(measure_one_column(src, buf_len, target, pos, col_width))) {
                LOG_WARN("measure target column failed", K(ret), K(ordinal), K(target));
              }
            }
          }
        } else {
          col_offset = pos;
          if (OB_FAIL(measure_one_column(src, buf_len, target, pos, col_width))) {
            LOG_WARN("measure target column failed", K(ret), K(ordinal), K(target));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogicalTableDecoder::decode_key_uint64_at_ordinal(
    const common::ObString &key,
    const ObLogicalTableAuxType aux_type,
    const int64_t ordinal,
    uint64_t &out)
{
  int ret = OB_SUCCESS;
  out = 0;
  int64_t col_offset = 0;
  int64_t col_width = 0;
  if (OB_FAIL(compute_key_column_offset(key, aux_type, ordinal, col_offset, col_width))) {
    LOG_WARN("compute key column offset failed", K(ret), K(aux_type), K(ordinal));
  } else if (OB_UNLIKELY(col_width != static_cast<int64_t>(sizeof(uint64_t)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("decode_key_uint64_at_ordinal: column width mismatch",
             K(ret), K(aux_type), K(ordinal), K(col_offset), K(col_width));
  } else {
    const unsigned char *src = reinterpret_cast<const unsigned char *>(key.ptr());
    int64_t pos = col_offset;
    if (OB_FAIL(ObLogicalTableCodecUtils::decode_uint64(src, key.length(), pos, out))) {
      LOG_WARN("decode_uint64 failed", K(ret), K(col_offset), K(aux_type), K(ordinal));
    }
  }
  return ret;
}

int ObLogicalTableDecoder::locate_key_cid_bytes_at_ordinal(
    const common::ObString &key,
    const ObLogicalTableAuxType aux_type,
    const int64_t ordinal,
    common::ObString &cid_bytes_out)
{
  int ret = OB_SUCCESS;
  cid_bytes_out.reset();
  int64_t col_offset = 0;
  int64_t col_width = 0;
  if (OB_FAIL(compute_key_column_offset(key, aux_type, ordinal, col_offset, col_width))) {
    LOG_WARN("compute key column offset failed", K(ret), K(aux_type), K(ordinal));
  } else if (OB_UNLIKELY(col_width != common::OB_DOC_ID_COLUMN_BYTE_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("locate_key_cid_bytes_at_ordinal: width is not 16B",
             K(ret), K(aux_type), K(ordinal), K(col_offset), K(col_width));
  } else if (OB_UNLIKELY(col_offset + col_width > key.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cid bytes out of key range",
             K(ret), K(col_offset), K(col_width), "key_len", key.length());
  } else {
    cid_bytes_out.assign_ptr(key.ptr() + col_offset, static_cast<int32_t>(col_width));
  }
  return ret;
}

int ObLogicalTableDecoder::decode_key_varchar_at_ordinal(
    const common::ObString &key,
    const ObLogicalTableAuxType aux_type,
    const int64_t ordinal,
    common::ObIAllocator &alloc,
    common::ObString &val_out)
{
  int ret = OB_SUCCESS;
  val_out.reset();
  int64_t col_offset = 0;
  int64_t col_width = 0;
  if (OB_FAIL(compute_key_column_offset(key, aux_type, ordinal, col_offset, col_width))) {
    LOG_WARN("compute key column offset failed", K(ret), K(aux_type), K(ordinal));
  } else {
    const ObLogicalTableKeyColDesc *col_descs = nullptr;
    int64_t col_cnt = 0;
    if (OB_FAIL(get_aux_type_col_descs(aux_type, col_descs, col_cnt))) {
      LOG_WARN("get aux type col descs failed", K(ret), K(aux_type));
    } else if (OB_UNLIKELY(ordinal >= col_cnt
                           || col_descs[ordinal].type_ != common::ObVarcharType)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("decode_key_varchar_at_ordinal: target column is not varchar",
               K(ret), K(aux_type), K(ordinal));
    } else if (col_descs[ordinal].is_order_preserving_encoded_) {
      // Order-preserving (16B) varchar: copy as-is, no escape decoding.
      if (OB_UNLIKELY(col_offset < 0 || col_width < 0 || col_offset + col_width > key.length())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("order-preserving varchar out of key range",
                 K(ret), K(col_offset), K(col_width), "key_len", key.length());
      } else {
        char *buf = static_cast<char *>(alloc.alloc(col_width));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc varchar buffer failed", K(ret), K(col_width));
        } else {
          MEMCPY(buf, key.ptr() + col_offset, col_width);
          val_out.assign_ptr(buf, static_cast<int32_t>(col_width));
        }
      }
    } else {
      // varchar_bin: unescape 0x00 0x01 -> 0x00 over [col_offset, col_offset+col_width).
      const unsigned char *src = reinterpret_cast<const unsigned char *>(key.ptr());
      // Worst-case unescaped length == col_width
      char *buf = static_cast<char *>(alloc.alloc(col_width > 0 ? col_width : 1));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc varchar buffer failed", K(ret), K(col_width));
      } else {
        int64_t out_pos = 0;
        int64_t scan = col_offset;
        const int64_t end = col_offset + col_width;
        while (scan < end) {
          if (scan + 1 < end && src[scan] == 0x00 && src[scan + 1] == 0x01) {
            buf[out_pos++] = 0x00;
            scan += 2;
          } else {
            buf[out_pos++] = static_cast<char>(src[scan]);
            scan += 1;
          }
        }
        val_out.assign_ptr(buf, static_cast<int32_t>(out_pos));
      }
    }
  }
  return ret;
}

int ObLogicalTableDecoder::decode_rowid(
  const common::ObString &key,
  uint64_t &rowid)
{
  int ret = OB_SUCCESS;
  const char *buf = key.ptr();
  const int64_t buf_len = key.length();
  int64_t pos = 0;
  ObLogicalTableKeyPrefix prefix;

  // Decode the prefix first (also self-validates aux_type from the key bytes),
  // then delegate the position + uint64 decode to the aux_type overload so the
  // rowid-position layout lives in a single place.
  if (OB_UNLIKELY(key.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty logical table key", K(ret));
  } else if (OB_FAIL(decode_key_prefix(buf, buf_len, pos, prefix))) {
    LOG_WARN("failed to decode logical table key prefix", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(decode_rowid(key, prefix.aux_type_, rowid))) {
    LOG_WARN("failed to decode logical table rowid", K(ret), K(buf_len), K(prefix.aux_type_));
  }

  return ret;
}

int ObLogicalTableDecoder::decode_rowid(
  const common::ObString &key,
  const ObLogicalTableAuxType aux_type,
  uint64_t &rowid)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = key.length();
  int64_t rowid_pos = 0;
  const unsigned char *src = reinterpret_cast<const unsigned char *>(key.ptr());

  if (OB_UNLIKELY(key.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty logical table key", K(ret));
  } else {
    switch (aux_type) {
      case LT_AUX_PRIMARY:
      case LT_AUX_FTS_FORWARD:
      case LT_AUX_IVF_ROWID_CID:
        rowid_pos = OB_LOGICAL_TABLE_KEY_PREFIX_LEN;
        break;
      case LT_AUX_SEARCH_INDEX:
      case LT_AUX_FTS_INVERTED:
      case LT_AUX_IVF_CID_ROWID:
        rowid_pos = buf_len - static_cast<int64_t>(sizeof(uint64_t));
        break;
      case LT_AUX_IVF_CID_VECTOR:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("aux type does not contain rowid", K(ret), K(aux_type));
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected aux type while decoding rowid", K(ret), K(aux_type));
        break;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(rowid_pos < 0
             || rowid_pos + static_cast<int64_t>(sizeof(uint64_t)) > buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowid position out of logical key range",
             K(ret), K(aux_type), K(rowid_pos), K(buf_len));
  } else if (OB_FAIL(ObLogicalTableCodecUtils::decode_uint64(src, buf_len, rowid_pos, rowid))) {
    LOG_WARN("failed to decode logical table rowid", K(ret), K(buf_len), K(rowid_pos));
  }

  return ret;
}

int ObLogicalTableDecoder::decode_value(
  common::ObIAllocator &allocator,
  const common::ObString &value_binary,
  common::ObIJsonBase *&value_root)
{
  int ret = OB_SUCCESS;
  value_root = nullptr;

  if (OB_FAIL(common::ObJsonBaseFactory::get_json_base(
    &allocator,
    value_binary,
    common::ObJsonInType::JSON_BIN,
    common::ObJsonInType::JSON_TREE,
    value_root))) {
    LOG_WARN("failed to parse logical table value", K(ret), K(value_binary));
  } else if (OB_ISNULL(value_root) || common::ObJsonNodeType::J_OBJECT != value_root->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("logical table value root must be a json object", K(ret), KP(value_root));
  }

  return ret;
}

int ObLogicalTableDecoder::decode_key_body(
    ObLogicalTableAuxType aux_type,
    const char *buf,
    const int64_t buf_len,
    int64_t &pos,
    common::ObIAllocator &allocator,
    common::ObIArray<common::ObDatum> &datums)
{
  int ret = OB_SUCCESS;
  const ObLogicalTableKeyColDesc *col_descs = nullptr;
  int64_t col_cnt = 0;
  const unsigned char *src = reinterpret_cast<const unsigned char *>(buf);

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0 || pos < 0 || pos > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for decode_key_body", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(get_aux_type_col_descs(aux_type, col_descs, col_cnt))) {
    LOG_WARN("failed to get key column descriptors", K(ret), K(aux_type));
  } else if (OB_ISNULL(col_descs) || OB_UNLIKELY(col_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty col_descs for aux_type", K(ret), K(aux_type));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      const ObLogicalTableKeyColDesc &col_desc = col_descs[i];
      common::ObDatum datum;

      if (col_desc.is_nullable_) {
        if (OB_UNLIKELY(pos >= buf_len)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("key body too short for nullable flag", K(ret), K(i), K(pos), K(buf_len));
        } else {
          uint8_t nullable = src[pos];
          pos += 1;
          if (0x00 == nullable) {
            datum.set_null();
            if (OB_FAIL(datums.push_back(datum))) {
              LOG_WARN("push null datum failed", K(ret), K(i));
            }
            continue;
          }
        }
      }

      if (OB_SUCC(ret)) {
        switch (col_desc.type_) {
          case common::ObUTinyIntType: {
            uint8_t val = 0;
            if (OB_FAIL(ObLogicalTableCodecUtils::decode_uint8(src, buf_len, pos, val))) {
              LOG_WARN("decode uint8 failed", K(ret), K(i));
            } else {
              char *storage = static_cast<char *>(allocator.alloc(sizeof(uint64_t)));
              if (OB_ISNULL(storage)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("alloc uint64 storage failed", K(ret), K(i));
              } else {
                datum.ptr_ = storage;
                datum.set_uint(static_cast<uint64_t>(val));
              }
            }
            break;
          }
          case common::ObUInt64Type: {
            uint64_t val = 0;
            if (OB_FAIL(ObLogicalTableCodecUtils::decode_uint64(src, buf_len, pos, val))) {
              LOG_WARN("decode uint64 failed", K(ret), K(i));
            } else {
              char *storage = static_cast<char *>(allocator.alloc(sizeof(uint64_t)));
              if (OB_ISNULL(storage)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("alloc uint64 storage failed", K(ret), K(i));
              } else {
                datum.ptr_ = storage;
                datum.set_uint(val);
              }
            }
            break;
          }
          case common::ObIntType: {
            int64_t val = 0;
            if (OB_FAIL(ObLogicalTableCodecUtils::decode_int64(src, buf_len, pos, val))) {
              LOG_WARN("decode int64 failed", K(ret), K(i));
            } else {
              char *storage = static_cast<char *>(allocator.alloc(sizeof(int64_t)));
              if (OB_ISNULL(storage)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("alloc int64 storage failed", K(ret), K(i));
              } else {
                datum.ptr_ = storage;
                datum.set_int(val);
              }
            }
            break;
          }
          case common::ObVarcharType: {
            if (col_desc.is_order_preserving_encoded_) {
              int64_t len = col_desc.fixed_length_;
              if (0 == len && i == col_cnt - 1) {
                len = buf_len - pos;
              }
              if (OB_UNLIKELY(len <= 0 || pos + len > buf_len)) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("invalid order-preserving varchar length",
                         K(ret), K(i), K(len), K(pos), K(buf_len));
              } else {
                char *str_buf = static_cast<char *>(allocator.alloc(len));
                if (OB_ISNULL(str_buf)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("alloc varchar buf failed", K(ret), K(len));
                } else {
                  MEMCPY(str_buf, src + pos, len);
                  datum.set_string(common::ObString(len, str_buf));
                  pos += len;
                }
              }
            } else {
              int64_t scan = pos;
              while (scan + 1 < buf_len && !(0x00 == src[scan] && 0x00 == src[scan + 1])) {
                scan += 1;
              }
              if (OB_UNLIKELY(scan + 1 >= buf_len)) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("varchar_bin terminator not found", K(ret), K(i), K(pos), K(buf_len));
              } else {
                const int64_t col_width = scan - pos;
                char *str_buf = static_cast<char *>(allocator.alloc(col_width > 0 ? col_width : 1));
                if (OB_ISNULL(str_buf)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("alloc varchar buf failed", K(ret), K(col_width));
                } else {
                  int64_t out_pos = 0;
                  scan = pos;
                  const int64_t end = pos + col_width;
                  while (scan < end) {
                    if (scan + 1 < end && 0x00 == src[scan] && 0x01 == src[scan + 1]) {
                      str_buf[out_pos++] = 0x00;
                      scan += 2;
                    } else {
                      str_buf[out_pos++] = static_cast<char>(src[scan]);
                      scan += 1;
                    }
                  }
                  datum.set_string(common::ObString(static_cast<int32_t>(out_pos), str_buf));
                  pos = end + 2;
                }
              }
            }
            break;
          }
          default:
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("unsupported key column type for decode", K(ret), K(i), K(col_desc));
            break;
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(datums.push_back(datum))) {
          LOG_WARN("push decoded datum failed", K(ret), K(i));
        }
      }
    }
  }

  return ret;
}

int ObLogicalTableDecoder::extract_column_from_value(
    common::ObIJsonBase *value_root,
    const common::ObString &column_name,
    common::ObString &column_value)
{
  int ret = OB_SUCCESS;
  column_value.reset();

  if (OB_ISNULL(value_root)
      || common::ObJsonNodeType::J_OBJECT != value_root->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value_root must be non-null J_OBJECT",
             K(ret), KP(value_root));
  } else if (column_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column_name is empty", K(ret));
  } else {
    common::ObIJsonBase *member = nullptr;
    if (OB_FAIL(value_root->get_object_value(column_name, member))) {
      if (common::OB_SEARCH_NOT_FOUND == ret) {
        // OB_ENTRY_NOT_EXIST is a contract return value (caller queries a column
        // that may not exist); keep it at TRACE to avoid log flooding.
        ret = OB_ENTRY_NOT_EXIST;
        LOG_TRACE("column not found in json value",
                 K(ret), K(column_name));
      } else {
        LOG_WARN("get_object_value failed", K(ret), K(column_name));
      }
    } else if (OB_ISNULL(member)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("json member is null", K(ret), K(column_name));
    } else if (common::ObJsonNodeType::J_STRING != member->json_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("json member is not a string",
               K(ret), K(column_name),
               "json_type", static_cast<int>(member->json_type()));
    } else {
      column_value.assign_ptr(
          const_cast<char *>(member->get_data()),
          static_cast<int32_t>(member->get_data_length()));
    }
  }

  return ret;
}

} // namespace share
} // namespace oceanbase
