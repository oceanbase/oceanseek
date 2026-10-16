/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "ob_shared_meta_util.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_sql_string.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;

int ObSharedMetaUtil::convert_cstr_to_hex(
    const ObString &input_str,
    ObString &output_str)
{
  int ret = OB_SUCCESS;
  int64_t ori_buf_len = input_str.length();
  const char *ori_buf = input_str.ptr();
  char *result_buf = nullptr;
  int64_t output_buf_len = 2 * ori_buf_len + 1;
  int64_t cstr_pos = 0;
  int64_t hex_pos = 0;

  if (nullptr == ori_buf || ori_buf_len <= 0) {
    output_str.assign(nullptr, 0); // discard '\0' at the end
  } else if (OB_ISNULL(result_buf = (char *)mtl_malloc(output_buf_len, "SSCstrToHex"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", KR(ret), K(output_buf_len));
  } else if (OB_FAIL(to_hex_cstr(ori_buf, ori_buf_len, result_buf, output_buf_len, hex_pos, cstr_pos))) {
    STORAGE_LOG(WARN, "fail to execute cstr to hex", KR(ret), K(ori_buf_len));
  } else {
    output_str.assign(result_buf, hex_pos - 1); // discard '\0' at the end
  }

  if (OB_FAIL(ret)) {
    mtl_free(result_buf);
    result_buf = nullptr;
  }
  return ret;
}

int ObSharedMetaUtil::convert_hex_to_cstr(
    const ObString &input_str,
    ObString &output_str)
{
  int ret = OB_SUCCESS;
  int64_t ori_buf_len = input_str.length();
  const char *ori_buf = input_str.ptr();
  char *result_buf = nullptr;
  int64_t output_buf_len = ori_buf_len / 2 + 1;
  int64_t pos = 0;

  if (OB_UNLIKELY(nullptr == ori_buf || ori_buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(input_str), K(ori_buf_len));
  } else if (OB_ISNULL(result_buf = (char *)mtl_malloc(output_buf_len, "SSHexToCstr"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", KR(ret), K(output_buf_len));
  } else if (OB_FAIL(hex_to_cstr(ori_buf, ori_buf_len, result_buf, output_buf_len, pos))) {
    STORAGE_LOG(WARN, "fail to execute cstr to hex", K(ret), K(ori_buf_len));
  } else {
    output_str.assign(result_buf, pos);
  }

  if (OB_FAIL(ret)) {
    mtl_free(result_buf);
    result_buf = nullptr;
  }
  return ret;
}

int ObSharedMetaUtil::escape_str_for_like(
    const ObString &ori_str,
    ObSqlString &escaped_str)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ori_str.length(); ++i) {
    char c = ori_str.ptr()[i];
    if (c == '%' || c == '_' || c == '!') {
      ret = escaped_str.append_fmt("!%c", c);
    } else if (c == '\'') {
      ret = escaped_str.append("''");
    } else {
      ret = escaped_str.append(&c, 1);
    }
  }
  return ret;
}

} /* namespace share */
} /* namespace oceanbase */