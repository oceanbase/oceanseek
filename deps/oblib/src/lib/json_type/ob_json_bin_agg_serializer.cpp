/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX LIB

#include "ob_json_bin_agg_serializer.h"
#include "ob_json_tree.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_array.h"
#include <algorithm>

namespace oceanbase {
namespace common {

bool ObJsonBinAggSerializer::EntryKeyLess::operator()(const Entry &a,
                                                      const Entry &b) const
{
  // Mirrors ObJsonKeyCompare(use_lexicographical_order_=false): shorter key
  // sorts first; equal-length keys compared lexicographically.
  if (a.key_.length() != b.key_.length()) {
    return a.key_.length() < b.key_.length();
  }
  return a.key_.compare(b.key_) < 0;
}

ObJsonBinAggSerializer::ObJsonBinAggSerializer(ObIAllocator &alloc, ObJsonNodeType root_type)
    : alloc_(alloc),
      root_type_(root_type),
      entries_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc, "JsonBinaryAgg"))
{
  // Only J_ARRAY/J_OBJECT make sense as standalone document roots. Scalars
  // go through ObJsonBinSerializer::serialize directly. We deliberately do
  // NOT silently rewrite an invalid root_type_ here: doing so would mask
  // caller bugs and turn the root_type_ checks in append_value /
  // append_key_value / serialize into dead branches. Instead we store the
  // value as given and let the first append_* (or serialize, if the caller
  // never appends) reject an invalid root_type_ with OB_ERR_UNEXPECTED,
  // failing fast at the earliest call site.
}

ObJsonBinAggSerializer::~ObJsonBinAggSerializer()
{
}

int ObJsonBinAggSerializer::append_value(const ObJsonWrapper &wrapper)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(root_type_ != ObJsonNodeType::J_ARRAY)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("append_value on non-array builder", K(ret), K(root_type_));
  } else if (OB_UNLIKELY(!wrapper.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json wrapper is invalid", K(ret));
  } else if (OB_FAIL(append_entry_common(ObString(), wrapper))) {
    LOG_WARN("append_entry_common fail", K(ret));
  }
  return ret;
}

int ObJsonBinAggSerializer::append_value(const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(root_type_ != ObJsonNodeType::J_ARRAY)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("append_value on non-array builder", K(ret), K(root_type_));
  } else if (OB_UNLIKELY(nullptr == value.ptr() && value.length() > 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("append_value string invalid", K(ret), K(value));
  } else {
    Entry entry;
    entry.key_.reset();
    entry.depth_ = 1; // scalar
    entry.type_ = static_cast<uint8_t>(ObJBVerType::J_STRING_V0);

    set_string(entry, value);
    if (OB_FAIL(entries_.push_back(entry))) {
      LOG_WARN("push back entry fail", K(ret));
    }
  }
  return ret;
}

int ObJsonBinAggSerializer::append_key_value(const ObString &key,
                                             const ObJsonWrapper &wrapper)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(root_type_ != ObJsonNodeType::J_OBJECT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("append_key_value on non-object builder", K(ret), K(root_type_));
  } else if (OB_UNLIKELY(!wrapper.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json wrapper is invalid", K(ret));
  } else if (OB_UNLIKELY(nullptr == key.ptr() && key.length() > 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json key is invalid", K(ret), K(key));
  } else if (OB_FAIL(append_entry_common(key, wrapper))) {
    LOG_WARN("append_entry_common fail", K(ret));
  }
  return ret;
}

int ObJsonBinAggSerializer::get_inline_value(const ObJsonWrapper &wrapper,
                                             uint64_t &value)
{
  int ret = OB_SUCCESS;
  value = 0;
  switch (wrapper.json_type()) {
    case ObJsonNodeType::J_NULL:
      value = 0;
      break;
    case ObJsonNodeType::J_BOOLEAN:
      value = static_cast<uint64_t>(wrapper.get_boolean());
      break;
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT:
      value = static_cast<uint64_t>(wrapper.get_int());
      break;
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG:
      value = wrapper.get_uint();
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObJsonBinAggSerializer::append_entry_common(const ObString &key,
                                                const ObJsonWrapper &wrapper)
{
  int ret = OB_SUCCESS;
  Entry entry;
  const uint32_t depth = wrapper.depth();
  entry.key_ = key;

  const ObJsonNodeType json_type = wrapper.json_type();
  if (OB_UNLIKELY(depth >= UINT16_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("json depth exceeds aggregate entry limit", K(ret), K(depth));
  } else if (FALSE_IT(entry.depth_ = static_cast<uint16_t>(depth))) {
  } else {
    entry.type_ = static_cast<uint8_t>(wrapper.vertype());
  }

  if (OB_FAIL(ret)) {
  } else if (is_inline_candidate_type(json_type)) {
    if (OB_FAIL(get_inline_value(wrapper, entry.inline_raw_))) {
      LOG_WARN("get_inline_value fail", K(ret), K(json_type));
    }
  } else {
    ObString value;
    bool is_raw_binary = false;
    if (OB_FAIL(wrapper.get_value_payload(alloc_, value, is_raw_binary))) {
      LOG_WARN("get_value_payload fail", K(ret), K(json_type));
    } else if (!is_raw_binary && value.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("non-candidate value has no binary payload", K(ret), K(json_type));
    } else {
      entry.value_ptr_ = value.ptr();
      entry.value_len_ = static_cast<uint32_t>(value.length());
      entry.is_raw_binary = is_raw_binary;
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(entries_.push_back(entry))) {
    LOG_WARN("push back entry fail", K(ret));
  }
  return ret;
}

int ObJsonBinAggSerializer::stable_sort()
{
  int ret = OB_SUCCESS;
  std::stable_sort(entries_.begin(), entries_.end(), EntryKeyLess{});
  return ret;
}

int ObJsonBinAggSerializer::unique()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(root_type_ == ObJsonNodeType::J_ARRAY)) {
    // pass
  } else if (entries_.count() > 1) {
    int64_t cur = 0;
    const int64_t last = entries_.count();
    for (int64_t pos = 1; pos < last; pos++) {
      const ObString &cur_key = entries_.at(cur).key_;
      const ObString &pos_key = entries_.at(pos).key_;
      if (cur_key.length() == pos_key.length() && cur_key.compare(pos_key) == 0) {
        // Duplicate: overwrite cur slot with the later value (last-wins).
        entries_.at(cur) = entries_.at(pos);
      } else {
        cur++;
        if (cur != pos) {
          entries_.at(cur) = entries_.at(pos);
        }
      }
    }
    while (++cur < last) {
      entries_.pop_back();
    }
  }
  return ret;
}

uint16_t ObJsonBinAggSerializer::depth() const
{
  uint16_t max_child = 0;
  // An entry with depth_ == 0 means the caller did not supply a depth; treat
  // it as a scalar (depth 1) so the container depth is still well-defined.
  for (int64_t i = 0; i < entries_.count(); i++) {
    uint16_t d = entries_.at(i).depth_;
    if (d == 0) {
      d = 1;
    }
    if (d > max_child) {
      max_child = d;
    }
  }
  return max_child + 1;
}

int ObJsonBinAggSerializer::serialize(ObStringBuffer &out)
{
  int ret = OB_SUCCESS;
  const int64_t start_pos = out.length();
  if (OB_UNLIKELY(start_pos != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("serialize output buffer is not empty", K(ret), K(start_pos));
  } else if (OB_UNLIKELY(root_type_ != ObJsonNodeType::J_ARRAY
                         && root_type_ != ObJsonNodeType::J_OBJECT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid root type", K(ret), K(root_type_));
  } else if (OB_FAIL(serialize_impl(out))) {
    LOG_WARN("serialize_impl fail", K(ret));
  }
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // serialize_impl() appends the document header before writing the
    // container body. Restore the caller-visible buffer on every failure.
    out.set_length(start_pos);
  }
  return ret;
}

int ObJsonBinAggSerializer::collect_entry_stats(uint64_t &key_data_len,
                                                uint64_t &total_value_bytes,
                                                uint64_t (&savings)[JBLS_MAX]) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < entries_.count(); ++i) {
    const Entry &entry = entries_.at(i);
    if (root_type_ == ObJsonNodeType::J_OBJECT) {
      const uint64_t key_len = static_cast<uint64_t>(entry.key_.length());
      key_data_len += key_len;
    }
    if (entry.is_inline_candidate()) {
      switch (ObJsonVerType::get_json_type(static_cast<ObJBVerType>(entry.type_))) {
        case ObJsonNodeType::J_NULL:
        case ObJsonNodeType::J_BOOLEAN:
          break;
        case ObJsonNodeType::J_INT:
        case ObJsonNodeType::J_OINT: {
          const int64_t signed_val = static_cast<int64_t>(entry.inline_raw_);
          const uint64_t value_len = serialization::encoded_length_vi64(signed_val);
          const uint8_t min_width = ObJsonVar::get_var_type(signed_val);
          total_value_bytes += value_len;
          savings[min_width] += value_len;
          break;
        }
        case ObJsonNodeType::J_UINT:
        case ObJsonNodeType::J_OLONG: {
          const uint64_t value_len =
              serialization::encoded_length_vi64(static_cast<int64_t>(entry.inline_raw_));
          const uint8_t min_width = ObJsonVar::get_var_type(entry.inline_raw_);
          total_value_bytes += value_len;
          savings[min_width] += value_len;
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid scalar candidate type", K(ret), K(entry.type_));
          break;
      }
    } else if (entry.is_raw_binary) {
      total_value_bytes += get_raw_string_binary_size(entry.value_len_);
    } else {
      total_value_bytes += entry.value_len_;
    }
  }
  return ret;
}

int ObJsonBinAggSerializer::calc_layout(uint64_t key_data_len,
                                        uint64_t total_value_bytes,
                                        const uint64_t (&savings)[JBLS_MAX],
                                        uint8_t &entry_var_type,
                                        uint64_t &obj_size) const
{
  int ret = OB_SUCCESS;
  const uint64_t element_count = entries_.count();
  const uint64_t count_size = ObJsonVar::get_var_size(ObJsonVar::get_var_type(element_count));
  const uint64_t fixed_size = OB_JSON_BIN_OBJ_HEADER_LEN + count_size + key_data_len;
  uint64_t all_savings = 0;
  for (int w = static_cast<int>(JBLS_UINT8); w <= static_cast<int>(JBLS_UINT64); ++w) {
    all_savings += savings[w];
  }

  const uint64_t min_offset_size = ObJsonBinMeta::calc_entry_array_size(root_type_ == ObJsonNodeType::J_OBJECT,
                                                                        element_count,
                                                                        static_cast<uint8_t>(JBLS_UINT8));
  const uint64_t min_value_data = total_value_bytes > all_savings
      ? total_value_bytes - all_savings
      : 0;
  const uint64_t min_total = fixed_size + 1 + min_offset_size + min_value_data;
  const int start_width = static_cast<int>(ObJsonVar::get_var_type(min_total));

  bool found = false;
  uint64_t cumulative_savings = 0;
  for (int w = static_cast<int>(JBLS_UINT8); w < start_width; ++w) {
    cumulative_savings += savings[w];
  }
  for (int w = start_width; !found && w <= static_cast<int>(JBLS_UINT64); ++w) {
    cumulative_savings += savings[w];
    const uint8_t candidate_var_type = static_cast<uint8_t>(w);
    const uint64_t entry_var_size = ObJsonVar::get_var_size(candidate_var_type);
    const uint64_t total_offset_size = ObJsonBinMeta::calc_entry_array_size(root_type_ == ObJsonNodeType::J_OBJECT,
                                                                            element_count,
                                                                            candidate_var_type);
    const uint64_t value_data = total_value_bytes - cumulative_savings;
    const uint64_t total = fixed_size + entry_var_size + total_offset_size + value_data;
    if (ObJsonVar::get_var_type(total) <= static_cast<uint8_t>(w)) {
      entry_var_type = static_cast<uint8_t>(w);
      obj_size = total;
      found = true;
    }
  }
  if (OB_UNLIKELY(!found)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to calculate json container layout", K(ret),
        K(element_count), K(total_value_bytes));
  }
  return ret;
}

int ObJsonBinAggSerializer::calc_obj_size(uint8_t &entry_var_type,
                                          uint64_t &obj_size,
                                          uint64_t &key_data_len) const
{
  int ret = OB_SUCCESS;
  uint64_t total_value_bytes = 0;
  uint64_t savings[JBLS_MAX] = {0};
  entry_var_type = static_cast<uint8_t>(JBLS_UINT8);
  obj_size = 0;
  key_data_len = 0;
  if (OB_FAIL(collect_entry_stats(key_data_len, total_value_bytes, savings))) {
    LOG_WARN("collect entry stats fail", K(ret));
  } else if (OB_FAIL(calc_layout(key_data_len, total_value_bytes, savings,
                                 entry_var_type, obj_size))) {
    LOG_WARN("calculate json container layout fail", K(ret));
  }
  return ret;
}

int ObJsonBinAggSerializer::add_container_header_v0(ObJsonBuffer &out,
                                                    uint8_t entry_var_type,
                                                    uint64_t obj_size,
                                                    ObJsonBinMeta &meta) const
{
  int ret = OB_SUCCESS;
  const uint64_t element_count = entries_.count();
  meta.set_type(root_type_ == ObJsonNodeType::J_OBJECT
                    ? ObJsonBin::get_object_vertype()
                    : ObJsonBin::get_array_vertype(),
                false);
  meta.set_element_count(element_count);
  meta.set_element_count_var_type(ObJsonVar::get_var_type(element_count));
  meta.set_obj_size(obj_size);
  meta.set_obj_size_var_type(entry_var_type);
  meta.set_entry_var_type(entry_var_type);
  meta.set_is_continuous(true);
  if (OB_FAIL(meta.calc_entry_array())) {
    LOG_WARN("calc_entry_array fail", K(ret));
  } else if (OB_FAIL(meta.to_header(out))) {
    LOG_WARN("write container header fail", K(ret));
  }
  return ret;
}

int ObJsonBinAggSerializer::add_keys(const ObJsonBinMeta &meta,
                                     char *buf_ptr,
                                     ObJsonBuffer &out) const
{
  int ret = OB_SUCCESS;
  if (root_type_ == ObJsonNodeType::J_OBJECT) {
    uint64_t key_offset = meta.get_entry_data_offset();
    const int64_t body_offset = buf_ptr - out.ptr();
    for (int64_t i = 0; OB_SUCC(ret) && i < entries_.count(); ++i) {
      const ObString &key = entries_.at(i).key_;
      char *current_buf_ptr = out.ptr() + body_offset;
      if (OB_FAIL(ObJsonBinSerializer::set_key_entry(meta, current_buf_ptr,
                                                     static_cast<int>(i), key_offset,
                                                     static_cast<uint64_t>(key.length()),
                                                     false))) {
        LOG_WARN("set key entry fail", K(ret), K(i));
      } else if (OB_FAIL(out.append(key.ptr(), key.length(), 0))) {
        LOG_WARN("append key fail", K(ret), K(i));
      } else {
        key_offset += key.length();
      }
    }
  }
  return ret;
}

int ObJsonBinAggSerializer::add_values(const ObJsonBinMeta &meta,
                                       char *buf_ptr,
                                       uint64_t key_data_len,
                                       ObJsonBuffer &out)
{
  int ret = OB_SUCCESS;
  const int64_t body_offset = buf_ptr - out.ptr();
  uint64_t cur_value_offset = meta.get_value_data_offset(key_data_len);
  for (int64_t i = 0; OB_SUCC(ret) && i < entries_.count(); ++i) {
    Entry &entry = entries_.at(i);
    char *current_buf_ptr = out.ptr() + body_offset;
    const ObJsonNodeType json_type =
        ObJsonVerType::get_json_type(static_cast<ObJBVerType>(entry.type_));
    if (json_type == ObJsonNodeType::J_NULL || json_type == ObJsonNodeType::J_BOOLEAN) {
      // Null and boolean values are always inlined, so they have no value data.
      entry.value_len_ = 0;
      const uint8_t vt = json_type == ObJsonNodeType::J_NULL
          ? ObJsonBin::get_null_vertype()
          : ObJsonBin::get_boolean_vertype();
      if (OB_FAIL(ObJsonBinSerializer::set_value_entry(meta, current_buf_ptr,
                                                       static_cast<int>(i),
                                                       entry.inline_raw_,
                                                       vt | OB_JSON_TYPE_INLINE_MASK,
                                                       false))) {
        LOG_WARN("set inline value entry fail", K(ret), K(i));
      }
    } else if (json_type == ObJsonNodeType::J_INT || json_type == ObJsonNodeType::J_OINT
               || json_type == ObJsonNodeType::J_UINT || json_type == ObJsonNodeType::J_OLONG) {
      // Integer values may be inlined or serialized, depending on their magnitude.
      const bool is_signed = (json_type == ObJsonNodeType::J_INT
                              || json_type == ObJsonNodeType::J_OINT);
      const int64_t signed_val = static_cast<int64_t>(entry.inline_raw_);
      const bool can_inline = is_signed ? (ObJsonVar::get_var_type(signed_val) <= meta.entry_var_type())
                                        : (ObJsonVar::get_var_type(entry.inline_raw_) <= meta.entry_var_type());
      if (can_inline) {
        entry.value_len_ = 0;
        const uint8_t vt = is_signed ? ObJsonBin::get_int_vertype()
                                     : ObJsonBin::get_uint_vertype();
        const uint64_t inlined_val = is_signed ? ObJsonVar::var_int2uint(signed_val)
                                               : entry.inline_raw_;
        if (OB_FAIL(ObJsonBinSerializer::set_value_entry(meta, current_buf_ptr,
                                                         static_cast<int>(i),
                                                         inlined_val,
                                                         vt | OB_JSON_TYPE_INLINE_MASK,
                                                         false))) {
          LOG_WARN("set inline value entry fail", K(ret), K(i));
        }
      } else {
        const int64_t pos_before = static_cast<int64_t>(out.length());
        int64_t pos = pos_before;
        if (OB_FAIL(serialization::encode_vi64(out.ptr(), out.capacity(), pos, signed_val))) {
          LOG_WARN("encode_vi64 fail", K(ret), K(i));
        } else {
          const uint32_t enc_len = static_cast<uint32_t>(pos - pos_before);
          entry.value_len_ = enc_len;
          if (OB_FAIL(out.set_length(static_cast<uint64_t>(pos)))) {
            LOG_WARN("set out length fail", K(ret), K(i));
          } else if (OB_FAIL(ObJsonBinSerializer::set_value_entry(meta, current_buf_ptr,
                                                                  static_cast<int>(i),
                                                                  cur_value_offset,
                                                                  entry.type_, false))) {
            LOG_WARN("set value entry fail", K(ret), K(i), K(cur_value_offset));
          } else {
            cur_value_offset += enc_len;
          }
        }
      }
    } else {
      // All other value types are always serialized.
      if (OB_FAIL(ObJsonBinSerializer::set_value_entry(meta, current_buf_ptr,
                                                       static_cast<int>(i),
                                                       cur_value_offset,
                                                       entry.type_, false))) {
        LOG_WARN("set value entry fail", K(ret), K(i), K(cur_value_offset));
      } else if (entry.is_raw_binary) {
        const uint64_t length_before = out.length();
        const uint64_t expected_len = get_raw_string_binary_size(entry.value_len_);
        const ObString value(static_cast<int32_t>(entry.value_len_), entry.value_ptr_);
        if (OB_FAIL(ObJsonBinSerializer::serialize_json_string(static_cast<ObJBVerType>(entry.type_), 
                                                               value, 
                                                               out))) {
          LOG_WARN("serialize raw string fail", K(ret), K(i), K(entry.value_len_));
        } else {
          const uint64_t actual_len = out.length() - length_before;
          if (OB_UNLIKELY(actual_len != expected_len)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("raw string serialized length mismatch", K(ret), K(i),
                     K(actual_len), K(expected_len));
          } else {
            cur_value_offset += actual_len;
          }
        }
      } else {
        if (OB_FAIL(out.append(entry.value_ptr_, entry.value_len_, 0))) {
          LOG_WARN("append binary value fail", K(ret), K(i), K(entry.value_len_));
        } else {
          cur_value_offset += entry.value_len_;
        }
      }
    }
  }
  return ret;
}

// Build the container body (everything after ObJsonBinDocHeader). Layout:
//   [ObJsonBinHeader (2 bytes: type + flags bitfield)]
//   [count var-size]
//   [obj_size var-size]
//   [key entries]   (object only: count * (entry_var_size * 2), reserved by to_header)
//   [value entries] (count * (entry_var_size + 1), reserved by to_header)
//   [key data]      (object only: concatenated keys)
//   [value data]    (non-inline values in entry order)
int ObJsonBinAggSerializer::serialize_impl(ObStringBuffer &out)
{
  int ret = OB_SUCCESS;
  const uint64_t doc_hdr_size = sizeof(ObJsonBinDocHeader);
  const int64_t start_pos = out.length();
  uint8_t entry_var_type = static_cast<uint8_t>(JBLS_UINT8);
  uint64_t key_data_len = 0;
  uint64_t obj_size = 0;
  ObJsonBinMeta meta;
  char *buf_ptr = NULL;

  if (OB_FAIL(calc_obj_size(entry_var_type, obj_size, key_data_len))) {
    LOG_WARN("calculate object size fail", K(ret));
  } else if (OB_FAIL(out.reserve(doc_hdr_size + obj_size))) {
    LOG_WARN("reserve json document fail", K(ret), K(obj_size));
  } else if (OB_FAIL(ObJsonBin::add_doc_header_v0(out))) {
    LOG_WARN("add_doc_header_v0 fail", K(ret));
  } else if (OB_FAIL(add_container_header_v0(out, entry_var_type, obj_size, meta))) {
    LOG_WARN("add container header fail", K(ret));
  } else if (OB_FALSE_IT(buf_ptr = out.ptr() + start_pos + doc_hdr_size)) {
  } else if (OB_FAIL(add_keys(meta, buf_ptr, out))) {
    LOG_WARN("add keys fail", K(ret));
  } else if (OB_FAIL(add_values(meta, buf_ptr, key_data_len, out))) {
    LOG_WARN("add values fail", K(ret));
  } else if (OB_FAIL(ObJsonBin::set_doc_header_v0(out, out.length(), false))) {
    LOG_WARN("set_doc_header_v0 fail", K(ret));
  } else if (OB_UNLIKELY(out.length() - start_pos != doc_hdr_size + obj_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregate obj_size mismatch", K(ret),
             K(out.length() - start_pos), K(doc_hdr_size), K(obj_size));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
