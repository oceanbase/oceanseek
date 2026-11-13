/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_STRING_OB_STRING_BUFFER_
#define OCEANBASE_LIB_STRING_OB_STRING_BUFFER_

#include "lib/string/ob_string.h"
#include "common/object/ob_obj_type.h"

namespace oceanbase {
namespace common {

class ObStringBuffer
{
public:
  static const int64_t STRING_BUFFER_INIT_STRING_LEN = 512;

  ObStringBuffer(common::ObIAllocator *allocator);
  // init_cap: hint for the first allocation; avoids the default 512-byte minimum for small outputs.
  ObStringBuffer(common::ObIAllocator *allocator, uint64_t init_cap);
  // Pre-allocated backing buffer.  buf must have at least `cap` usable bytes.
  // allocator is used ONLY when the buffer overflows and needs to grow.
  // Callers using arena allocators: arena->free() is a no-op, so it is safe for reset()/extend()
  // to call allocator->free(buf) when buf was not originally allocated by that allocator.
  ObStringBuffer(char *buf, uint64_t cap, common::ObIAllocator *allocator);
  ObStringBuffer();
  ~ObStringBuffer();
  // before set new alloctor, should call reset() first
  inline void set_allocator(common::ObIAllocator *allocator)
  {
    allocator_ = allocator;
  }
  common::ObIAllocator *get_allocator() const { return allocator_; }
  void reset();
  void reuse();
  int get_result_string(ObString &buffer);

  int append(const char *str);

  // Append data later
  // @param [in] flag reserves flag bits for other subsequent expansions
  // @return Returns OB_SUCCESS on success, error code otherwise.
  // When the flag is the default value -1, the size of len_*2 + len is used each time to expand the memory.
  // If the data is large, the default value is not used, and the memory application size is len_ + len.
  int append(const char *str, const uint64_t len, int8_t flag = -1);
  int append(const ObString &str, int8_t flag = -1);
  int reserve(const uint64_t len);
  int extend(const uint64_t len);

  const char *ptr() const { return data_; }
  uint64_t length() const { return len_; }
  uint64_t capacity() const { return cap_ ; }
  inline uint64_t remain() const
  {
    return OB_LIKELY(cap_ > 0) ? (cap_ - len_) : cap_;
  }
  bool empty() const { return 0 == length(); }
  char *ptr() { return data_; }
  int set_length(const uint64_t len);
  // Update the length now; add the '\0' later.
  // Add the trailing '\0' when done, even on errors.
  OB_INLINE int append_unterminated(const char *str, uint64_t size, int8_t flag = 0)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_NULL_VALUE;
    } else if (OB_UNLIKELY(size > INT64_MAX)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_NOT_NULL(str)) {
      const uint64_t need_len = len_ + size;
      const uint64_t reserve_size = flag == -1 ? need_len : size;
      if (OB_UNLIKELY(need_len < len_)) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_UNLIKELY(reserve_size > INT64_MAX - 8
                            || len_ > INT64_MAX - 8 - reserve_size)) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        if (OB_UNLIKELY(len_ + reserve_size + 8 > cap_)) {
          ret = reserve(reserve_size);
        }
        if (OB_SUCC(ret)) {
          MEMCPY(data_ + len_, str, size);
          len_ = need_len;
        }
      }
    }
    return ret;
  }

  OB_INLINE int set_length_unterminated(uint64_t size)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(size > cap_ || (size == cap_ && size != 0))) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      len_ = size;
    }
    return ret;
  }

  char back() { return data_[len_ - 1]; };
  const ObString string() const;
  int deep_copy(common::ObIAllocator *allocator, ObStringBuffer &input);
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "len = %ld, cap = %ld", len_, cap_);
    return pos;
  }
private:
  common::ObIAllocator *allocator_;
  char *data_;
  uint64_t len_;
  uint64_t cap_;
  uint64_t init_cap_;

  DISALLOW_COPY_AND_ASSIGN(ObStringBuffer);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_STRING_OB_STRING_BUFFER_
