/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_STRING_OB_SENSITIVE_STRING
#define OCEANBASE_LIB_STRING_OB_SENSITIVE_STRING

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{
// ObSensitiveString Wraps a string containing sensitive information
// ensuring the access_key is obscured during printing.
class ObSensitiveString
{
public:
  ObSensitiveString(const char *str) : str_(str) {}
  ObSensitiveString(const ObString &str) : str_(str) {}
  int64_t to_string(char *buf, const int64_t len) const 
  {
    ObArenaAllocator allocator(ObModIds::OB_LOG);
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    char *ptr = nullptr;
    if (OB_LIKELY(nullptr != buf) && OB_LIKELY(len > 0)) {
      // Print "NULL" when str_ is nullptr, and "" when it's empty. 
      // Note that ob_dup_cstring will return nullptr even if an empty string is passed,
      // making it impossible to distinguish between an empty string and a null pointer. 
      if (str_.ptr() == nullptr) {
        (void)logdata_printf(buf, len, pos, "NULL");
      } else if (str_.length() == 0) {
        (void)logdata_printf(buf, len, pos, ""); 
      } else if (OB_FAIL(ob_dup_cstring(allocator, str_, ptr))) {
        LIB_LOG(ERROR, "ob_dup_cstring failed", K(ret));
      } else {
        if (OB_ISNULL(ptr)) {
          (void)logdata_printf(buf, len, pos, "NULL");
        } else {
          const char *access_key = "access_key";
          const char *scheme_end = STRSTR(ptr, "://");
          bool is_uri = nullptr != scheme_end && scheme_end > ptr
              && (('a' <= *ptr && 'z' >= *ptr) || ('A' <= *ptr && 'Z' >= *ptr));
          for (const char *scheme_pos = ptr + 1; is_uri && scheme_pos < scheme_end; ++scheme_pos) {
            is_uri = ('a' <= *scheme_pos && 'z' >= *scheme_pos)
                || ('A' <= *scheme_pos && 'Z' >= *scheme_pos)
                || ('0' <= *scheme_pos && '9' >= *scheme_pos)
                || '+' == *scheme_pos || '-' == *scheme_pos || '.' == *scheme_pos;
          }
          const char *query_pos = is_uri ? STRCHR(scheme_end + 3, '?') : nullptr;
          const char *print_pos = ptr;
          const char *search_pos = OB_ISNULL(query_pos) ? ptr : query_pos + 1;
          const char *start_pos = nullptr;
          while (nullptr != (start_pos = strcasestr(search_pos, access_key))) {
            const char *end_pos = start_pos;
            while ('\0' != *end_pos && '&' != *end_pos) {
              ++end_pos;
            }
            const int64_t prefix_length = start_pos - print_pos;
            (void)logdata_printf(buf, len, pos, "%.*s", static_cast<int32_t>(MIN(prefix_length, len)), print_pos);
            (void)logdata_printf(buf, len, pos, "access_key={hidden_access_key}");
            print_pos = end_pos;
            search_pos = '\0' == *end_pos ? end_pos : end_pos + 1;
          }
          (void)logdata_printf(buf, len, pos, "%s", print_pos);
        }
      }
    }
    return pos;
  }
private:
  const ObString str_;
};
} // end namespace common
} // end namespace oceanbase

#endif