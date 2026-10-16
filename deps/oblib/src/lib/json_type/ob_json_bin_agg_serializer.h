/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_LIB_JSON_TYPE_OB_JSON_BIN_AGG_SERIALIZER_H
#define OCEANBASE_LIB_JSON_TYPE_OB_JSON_BIN_AGG_SERIALIZER_H

#include "ob_json_wrapper.h"
#include "ob_json_bin.h"
#include "lib/container/ob_array.h"

namespace oceanbase {
namespace common {

// Stream-style builder for the standard JSONB standalone binary,
// byte-for-byte compatible with ObJsonBin::get_raw_binary. Inputs are
// ObJsonWrapper elements dispatched per representation (bin-view / ObJsonBin
// fallback / ObJsonNode DOM), so JSON_ARRAY/JSON_OBJECT can build the result
// without an ObJsonArray/ObJsonObject DOM intermediate.
class ObJsonBinAggSerializer
{
public:
  ObJsonBinAggSerializer(ObIAllocator &alloc, ObJsonNodeType root_type);
  ~ObJsonBinAggSerializer();

  // Appends one array element. Both continuous binary value-areas and plain
  // J_STRING values are held as shallow references; the caller must keep the
  // underlying data alive until serialize() returns. Other Tree DOM values are
  // serialized into allocator-owned stable storage during the append phase.
  //
  // The JSON depth of the value is provided by ObJsonWrapper::depth().
  int append_value(const ObJsonWrapper &wrapper);

  // Directly appends a string element, skipping the ObJsonString DOM
  // allocation. Only a shallow reference to value is held; the
  // [vertype][varint length][raw bytes] are written into the final output in
  // one shot during serialize(). The caller must keep value alive until
  // serialize() returns, ensure it does not overlap with the output buffer,
  // and that value is valid UTF-8.
  int append_value(const ObString &value);

  // Append one object member. The key ObString is copied by reference; the
  // caller must keep the underlying buffer alive until serialize() returns.
  // See append_value for the shallow-copy and wrapper-consumption semantics.
  int append_key_value(const ObString &key, const ObJsonWrapper &wrapper);

  // Stable sort entries_ by key_. No-op for J_ARRAY (array elements have no
  // key and MySQL semantics do not require sorting). O(n log n) via
  // std::stable_sort; idempotent (already-sorted input is still O(n log n)).
  int stable_sort();

  // Adjacent-key last-wins deduplication. Drops every entry whose key equals
  // the previous kept entry's key, keeping the LAST value per key. Matches
  // ObJsonObject::unique semantics. Does NOT require a prior stable_sort() --
  // follows std::unique's contract that the caller is responsible for putting
  // equivalent keys adjacent (typically via stable_sort() first). Idempotent.
  int unique();

  // Container depth: max(entry.depth_) + 1 (the +1 accounts for this container
  // itself). Returns 1 when entries_ is empty (a container with no children
  // has depth 1). Entries with depth_ == 0 are treated as depth 1 (scalar).
  uint16_t depth() const;

  // Finalize and write the standalone binary into the caller-owned, empty
  // `out` buffer. On failure `out` is restored to its original length.
  //
  // Storage responsibility: `out` is owned by the caller and must outlive any
  // consumer of the produced bytes (e.g. the ObString view returned by
  // out.string() is a shallow reference into out's storage). This class does
  // NOT hold the produced buffer -- it only writes into `out` -- so the result
  // is safe regardless of whether `out`'s allocator is an arena (no-op free)
  // or a real freeing allocator, as long as `out` itself stays alive.
  int serialize(ObStringBuffer &out);

  OB_INLINE uint32_t element_count() const
  {
    return static_cast<uint32_t>(entries_.count());
  }

private:
  // Whether a JSON node type is one of the six inline candidates
  // (null/bool/int/oint/uint/olong). Pure type test, no instance state.
  OB_INLINE static bool is_inline_candidate_type(ObJsonNodeType t)
  {
    return t == ObJsonNodeType::J_NULL
        || t == ObJsonNodeType::J_BOOLEAN
        || t == ObJsonNodeType::J_INT
        || t == ObJsonNodeType::J_OINT
        || t == ObJsonNodeType::J_UINT
        || t == ObJsonNodeType::J_OLONG;
  }

  // Per-element staging record. Inline candidate types use inline_raw_;
  // other types use value_ptr_. When is_raw_binary is true, value_ptr_/
  // value_len_ denote the raw string without the [vertype][varint length]
  // header; otherwise they denote ready-to-append JSONB value-area data.
  // When an inline candidate ends up not inlined, serialize_impl() updates
  // value_len_ to the encoded value-data length.
  struct Entry
  {
    Entry()
        : key_(),
          inline_raw_(0),
          value_len_(0),
          depth_(0),
          type_(static_cast<uint8_t>(J_ERROR_V0)),
          is_raw_binary(false)
    {}

    ObString key_;        // empty for array elements
    union {
      uint64_t inline_raw_;
      const char *value_ptr_;
    };
    uint32_t value_len_;
    uint16_t depth_;      // 0 = unknown, treated as scalar depth 1
    uint8_t type_;        // JSONB vertype (without inline mask)
    bool is_raw_binary;   // raw string (true) or ready-to-use binary (false)

    OB_INLINE bool is_inline_candidate() const
    {
      return ObJsonBinAggSerializer::is_inline_candidate_type(
          ObJsonVerType::get_json_type(static_cast<ObJBVerType>(type_)));
    }

    // ObArray instantiates ObIArray<T>::to_string, which requires T to expose
    // a to_string; provide a minimal one so the template instantiation compiles.
    int64_t to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      BUF_PRINTO(key_);
      if (is_inline_candidate()) {
        BUF_PRINTO(inline_raw_);
      } else {
        BUF_PRINTO(value_ptr_);
      }
      BUF_PRINTO(value_len_);
      BUF_PRINTO(depth_);
      BUF_PRINTO(type_);
      BUF_PRINTO(is_raw_binary);
      return pos;
    }
  };

  // Strict-weak-ordering comparator for sorting Entry by key_.
  // Mirrors ObJsonKeyCompare(use_lexicographical_order_=false): shorter key
  // sorts first; equal-length keys compared lexicographically. Returns bool
  // (not int) to satisfy std::stable_sort's Compare requirement.
  struct EntryKeyLess {
    bool operator()(const Entry &a, const Entry &b) const;
  };

  // Common dispatch: store a candidate scalar raw value, shallow-copy a
  // continuous binary value, or serialize a tree DOM into stable storage.
  // depth is stored into the Entry so depth() can report container depth after
  // append.
  int append_entry_common(const ObString &key, const ObJsonWrapper &wrapper);

  // Records a plain J_STRING as a shallow reference, deferring the actual
  // write into the final buffer until serialize(). is_raw_binary is true,
  // meaning value_ptr_/value_len_ point to raw string bytes without the
  // [vertype][varint length] header. The caller must have validated the
  // string (no nullptr with non-zero length) before calling.
  OB_INLINE static void set_string(Entry &entry, const ObString &value)
  {
    entry.value_ptr_ = value.ptr();
    entry.value_len_ = static_cast<uint32_t>(value.length());
    entry.is_raw_binary = true;
  }

  // Computes the exact length of a raw string when written into the JSONB value-area.
  OB_INLINE static uint64_t get_raw_string_binary_size(uint32_t value_len)
  {
    return sizeof(uint8_t)
        + serialization::encoded_length_vi64(static_cast<int64_t>(value_len))
        + static_cast<uint64_t>(value_len);
  }

  // Extract the raw uint64 bit pattern of an inline-candidate scalar from
  // the wrapper (bin or dom). Caller MUST gate on
  // is_inline_candidate_type() first; reaching the default branch is a
  // programming error and returns OB_ERR_UNEXPECTED.
  static int get_inline_value(const ObJsonWrapper &wrapper, uint64_t &value);

  // Collect statistics from entries_ in a single scan for layout calculation.
  int collect_entry_stats(uint64_t &key_data_len,
                         uint64_t &total_value_bytes,
                         uint64_t (&savings)[JBLS_MAX]) const;

  // Pick the smallest entry width that can hold the exact object size
  // calculated with the inline savings enabled by that width.
  int calc_layout(uint64_t key_data_len,
                  uint64_t total_value_bytes,
                  const uint64_t (&savings)[JBLS_MAX],
                  uint8_t &entry_var_type,
                  uint64_t &obj_size) const;

  // Calculate the final entry width and object size. key_data_len is returned
  // as an auxiliary layout input used later when value offsets are written.
  int calc_obj_size(uint8_t &entry_var_type,
                    uint64_t &obj_size,
                    uint64_t &key_data_len) const;

  // Initialize the container metadata and append the V0 container header,
  // including the reserved key/value entry arrays.
  int add_container_header_v0(ObJsonBuffer &out,
                              uint8_t entry_var_type,
                              uint64_t obj_size,
                              ObJsonBinMeta &meta) const;

  // Fill the reserved key-entry array and append the corresponding key data
  // in a single pass. Array containers are treated as a no-op.
  int add_keys(const ObJsonBinMeta &meta,
               char *buf_ptr,
               ObJsonBuffer &out) const;

  // Fill the reserved value-entry array and append non-inline value data in
  // a single pass.
  int add_values(const ObJsonBinMeta &meta,
                 char *buf_ptr,
                 uint64_t key_data_len,
                 ObJsonBuffer &out);

  // Write the document header (ObJsonBinDocHeader) and the container body.
  // The entry width is computed exactly by scanning entries once and
  // enumerating the four candidate widths (UINT8/16/32/64), so no reserialize
  // pass is needed.
  int serialize_impl(ObStringBuffer &out);

  ObIAllocator &alloc_;
  ObJsonNodeType root_type_;        // J_ARRAY or J_OBJECT
  ObArray<Entry> entries_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_JSON_TYPE_OB_JSON_BIN_AGG_SERIALIZER_H
