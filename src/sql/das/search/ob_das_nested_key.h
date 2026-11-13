/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_DAS_SEARCH_OB_DAS_NESTED_KEY_H_
#define OCEANBASE_SQL_DAS_SEARCH_OB_DAS_NESTED_KEY_H_

#include <endian.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace sql
{
// A position within the current document. Short keys are inline; long keys
// borrow their bytes until the owning child moves or resets its cursor.
class ObDASNestedKey
{
public:
  // Short NKs retain byte-string order as an unsigned integer. Use memcpy for
  // unaligned storage keys; a one-level key occupies the high 32 bits.
  static uint64_t load(const char *key, const int32_t bytes)
  {
    uint64_t value = 0;
    if (4 == bytes) {
      uint32_t prefix = 0;
      MEMCPY(&prefix, key, sizeof(prefix));
      value = static_cast<uint64_t>(be32toh(prefix)) << 32;
    } else {
      MEMCPY(&value, key, sizeof(value));
      value = be64toh(value);
    }
    return value;
  }
  static void store(char *key, const uint64_t value, const int32_t bytes)
  {
    const uint64_t encoded = htobe64(value);
    MEMCPY(key, &encoded, bytes);
  }

  ObDASNestedKey() : inline_key_(0), length_(0), flags_(FLAG_MIN) {}
  void reset() { set_min(); }
  void set_min() { inline_key_ = 0; length_ = 0; flags_ = FLAG_MIN; }
  void set_max() { inline_key_ = 0; length_ = 0; flags_ = FLAG_MAX; }
  bool is_min() const { return flags_ & FLAG_MIN; }
  bool is_max() const { return flags_ & FLAG_MAX; }
  bool is_normal() const { return 0 == flags_; }
  uint16_t length() const { return length_; }
  uint64_t value() const { return inline_key_; }
  const char *ptr() const { return key_ptr_; }
  void set_value(uint64_t value, uint16_t length)
  {
    inline_key_ = value;
    length_ = length;
    flags_ = 0;
  }
  void assign(const char *key, uint16_t length)
  {
    if (length <= sizeof(uint64_t)) {
      inline_key_ = load(key, length);
    } else {
      key_ptr_ = key;
    }
    length_ = length;
    flags_ = 0;
  }
  // Construct a new NK seek target with a caller-selected effective length.
  // Forwarding an existing target does not change its length.
  void assign(const ObDASNestedKey &key, uint16_t length)
  {
    *this = key;
    if (is_normal()) {
      if (length <= sizeof(uint64_t)) {
        const uint64_t value = length_ <= sizeof(uint64_t)
            ? inline_key_ : load(key_ptr_, length);
        set_value(4 == length ? value & 0xffffffff00000000ULL : value, length);
      } else {
        length_ = length;
      }
    }
  }
  int compare(const ObDASNestedKey &other, int32_t compare_bytes) const
  {
    int cmp = 0;
    if (is_max() || other.is_max()) {
      cmp = static_cast<int>(is_max()) - static_cast<int>(other.is_max());
    } else if (is_min() || other.is_min()) {
      cmp = static_cast<int>(other.is_min()) - static_cast<int>(is_min());
    } else {
      if (compare_bytes <= sizeof(uint64_t)) {
        uint64_t lhs = length_ <= sizeof(uint64_t)
            ? value() : load(ptr(), compare_bytes);
        uint64_t rhs = other.length_ <= sizeof(uint64_t)
            ? other.value() : load(other.ptr(), compare_bytes);
        if (4 == compare_bytes) {
          lhs &= 0xffffffff00000000ULL;
          rhs &= 0xffffffff00000000ULL;
        }
        cmp = (lhs > rhs) - (lhs < rhs);
      } else {
        cmp = MEMCMP(key_ptr_, other.key_ptr_, compare_bytes);
      }
    }
    return cmp;
  }
  TO_STRING_KV(K_(inline_key), K_(length), K_(flags));
private:
  static constexpr uint16_t FLAG_MIN = 1 << 0;
  static constexpr uint16_t FLAG_MAX = 1 << 1;
  union {
    uint64_t inline_key_;
    const char *key_ptr_;
  };
  uint16_t length_;
  uint16_t flags_;
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_DAS_SEARCH_OB_DAS_NESTED_KEY_H_
