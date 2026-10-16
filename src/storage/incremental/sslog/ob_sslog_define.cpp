/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_sslog_define.h"
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"
#include "deps/oblib/src/lib/utility/serialization.h"

namespace oceanbase
{
namespace sslog
{
using namespace share;
using namespace common;


ObSSLogWriteParam::ObSSLogWriteParam(const bool only_update_extra_info,
                                     const bool check_extra_info,
                                     const ObString &extra_info)
  : only_update_extra_info_(only_update_extra_info),
    check_extra_info_(check_extra_info),
    extra_info_(extra_info)
{}

#define CHECK_FREE_OUTPUT_STR(output_str) \
  if (!reuse_buf) { /* do nothing when not reuse buf */ \
  } else if (nullptr != output_str.ptr() && output_str.length() >= output_buf_len) { \
    need_alloc_buf = false; \
    result_buf = output_str.ptr(); \
  } else if (nullptr != output_str.ptr()) { \
    mtl_free(output_str.ptr()); \
  }

int convert_cstr_to_hex(const ObString &input_str,
                        const bool reuse_buf,
                        ObString &output_str)
{
  int ret = OB_SUCCESS;
  int64_t ori_buf_len = input_str.length();
  const char *ori_buf = input_str.ptr();
  char *result_buf = nullptr;
  int64_t output_buf_len = 2 * ori_buf_len + 1;
  int64_t cstr_pos = 0;
  int64_t hex_pos = 0;
  bool need_alloc_buf = true;
  // 1. check output str buf enough or not
  CHECK_FREE_OUTPUT_STR(output_str)
  if (nullptr == ori_buf || ori_buf_len <= 0) {
    output_str.assign(nullptr, 0); // discard '\0' at the end
  } else if (OB_ISNULL(result_buf = (char *)share::mtl_malloc(output_buf_len, "SSCstrToHex"))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "alloc memory fail", K(ret));
  } else if (OB_FAIL(common::to_hex_cstr(ori_buf, ori_buf_len, result_buf, output_buf_len, hex_pos, cstr_pos))) {
    STORAGE_LOG(WARN, "cstr to hex fail", K(ret), K(ori_buf_len));
  } else {
    output_str.assign(result_buf, hex_pos - 1); // discard '\0' at the end
  }
  if (OB_FAIL(ret)) {
    share::mtl_free(result_buf);
    result_buf = nullptr;
  }
  return ret;
}

int convert_hex_to_cstr(const ObString &input_str,
                        const bool reuse_buf,
                        ObString &output_str)
{
  int ret = OB_SUCCESS;
  int64_t ori_buf_len = input_str.length();
  const char *ori_buf = input_str.ptr();
  char *result_buf = nullptr;
  int64_t output_buf_len = ori_buf_len / 2 + 1;
  int64_t pos = 0;
  bool need_alloc_buf = true;
  // 1. check output str buf enough or not
  CHECK_FREE_OUTPUT_STR(output_str)
  // 2. malloc new buf and do hex to cstr
  if (nullptr == ori_buf || ori_buf_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "err unexpected happen", K(ret), K(input_str), K(ori_buf_len));
  } else if (need_alloc_buf && OB_ISNULL(result_buf = (char *)share::mtl_malloc(output_buf_len, "SSHexToCstr"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "alloc memory fail", K(ret));
  } else if (OB_FAIL(common::hex_to_cstr(ori_buf, ori_buf_len, result_buf, output_buf_len, pos))) {
    STORAGE_LOG(WARN, "cstr to hex fail", K(ret), K(ori_buf_len), K(need_alloc_buf));
  } else {
    output_str.assign(result_buf, pos);
  }
  if (OB_FAIL(ret)) {
    share::mtl_free(result_buf);
    result_buf = nullptr;
  }
  return ret;
}

} // namespace sslog
} // namespace oceanbase
